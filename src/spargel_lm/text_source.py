import gzip
import json
import logging
import os
from pathlib import Path
from typing import Any, Iterator, Literal, override

import regex
from pydantic import BaseModel, Field
from regex import Pattern

from .typing import StrOrPath

logger = logging.getLogger(__name__)


def _resolve_parent(path: StrOrPath):
    p = Path(path).resolve()
    return p.parent if p.is_file() else p


#### Text sources ####


class TextSourceModel(BaseModel):
    """Base class for text source models"""

    comment: str = ""

    def get_texts(self, this_path: StrOrPath) -> Iterator[str]: ...


class FindFileSource(TextSourceModel):
    """Find files in a directory."""

    type: Literal["find"]
    base: str = "."
    paths: list[str] = ["."]
    file_pattern: str | None = None
    dir_pattern: str | None = None

    @override
    def get_texts(self, this_path):
        dir_pattern = regex.compile(self.dir_pattern) if self.dir_pattern else None
        file_pattern = regex.compile(self.file_pattern) if self.file_pattern else None

        base_path = _resolve_parent(this_path) / self.base

        for path in self.paths:
            yield from (
                str(p)
                for p in self._search_dir(
                    base_path / path, file_pattern=file_pattern, dir_pattern=dir_pattern
                )
            )

    def _search_dir(
        self,
        dir: StrOrPath,
        *,
        file_pattern: Pattern[str] | None = None,
        dir_pattern: Pattern[str] | None = None,
    ) -> Iterator[Path]:
        for cur_dir, dirs, files in os.walk(dir):
            if dir_pattern is not None:
                dirs[:] = [d for d in dirs if dir_pattern.fullmatch(d)]

            for file in files:
                if file_pattern is not None and not file_pattern.fullmatch(file):
                    continue
                yield Path(cur_dir) / file


class PlainTextSource(TextSourceModel):
    """Provide a list of plain texts directly."""

    type: Literal["text"]
    texts: list[str]

    @override
    def get_texts(self, this_path):
        yield from self.texts


class ProcessSource(TextSourceModel):
    """Apply operations to texts from sources."""

    type: Literal["process"]
    operations: list[Operation]
    sources: list[TextSource]

    @override
    def get_texts(self, this_path):
        for source in self.sources:
            for text in source.get_texts(this_path):
                texts = [text]
                for operation in self.operations:
                    tmp: list[str] = []
                    for text in texts:
                        tmp.extend(operation.process(text, this_path))
                    texts = tmp

                yield from texts


type TextSource = FindFileSource | PlainTextSource | ProcessSource


class TextSourceWrapperModel(BaseModel):
    source: TextSource = Field(discriminator="type")


#### Operations on texts ####


class OperationModel(BaseModel):
    """Base class for text operation models"""

    comment: str = ""

    def process(self, text: str, this_path: StrOrPath) -> Iterator[str]: ...


class ReadFileOperation(OperationModel):
    """Read file content."""

    type: Literal["read_file"]
    base: str = "."
    encoding: str | None = None
    compression: Literal["gzip"] | None = None

    @override
    def process(self, text: str, this_path):
        path = _resolve_parent(this_path) / self.base / text
        try:
            match self.compression:
                case "gzip":
                    f = gzip.open(path, "rt", encoding=self.encoding)
                case _:
                    f = open(path, "r", encoding=self.encoding)

            yield f.read()

            f.close()
        except UnicodeDecodeError:
            logger.warning(f'UnicodeDecodeError in file: "{path}"')


class ReferenceOperation(OperationModel):
    """Reference external operations."""

    type: Literal["ref"]
    base: str = "."
    paths: list[str]

    @override
    def process(self, text: str, this_path):
        texts = [text]
        for path in self.paths:
            ref_path = _resolve_parent(this_path) / self.base / path
            for operation in self._get_operations(ref_path):
                tmp: list[str] = []
                for text in texts:
                    tmp.extend(operation.process(text, ref_path))
                texts = tmp

        yield from texts

    def _get_operations(self, path: StrOrPath) -> Iterator[Operation]:
        with open(path, "r") as f:
            array = json.load(f)

        for obj in array:
            yield OperationWrapperModel.model_validate({"operation": obj}).operation


class ReplaceOperation(OperationModel):
    """Replace All"""

    type: Literal["replace"]
    regex: bool = False
    old: str
    new: str
    repeat: bool = False
    per_line: bool = False

    @override
    def process(self, text: str, this_path):
        if self.per_line:
            yield "\n".join(self._apply_replace(line) for line in text.splitlines())
        else:
            yield self._apply_replace(text)

    def _apply_replace(self, text: str) -> str:
        if self.repeat:
            last_text = text
            while True:
                if self.regex:
                    text = regex.sub(self.old, self.new, text)
                else:
                    text = text.replace(self.old, self.new)

                if text == last_text:
                    break
                last_text = text
            return text
        else:
            if self.regex:
                return regex.sub(self.old, self.new, text)
            else:
                return text.replace(self.old, self.new)


class RightStripOperation(OperationModel):
    """Right-strip text (remove trailing whitespace)."""

    type: Literal["rstrip"]
    chars: str | None = None
    per_line: bool = False

    @override
    def process(self, text: str, this_path):
        if self.per_line:
            yield "\n".join(line.rstrip(self.chars) for line in text.splitlines())
        else:
            yield text.rstrip(self.chars)


class SplitLinesOperation(OperationModel):
    """Split text into lines."""

    type: Literal["split_lines"]
    keep_ends: bool = False

    @override
    def process(self, text: str, this_path):
        yield from text.splitlines(keepends=self.keep_ends)


class StripOperation(OperationModel):
    """Strip text (remove leading and trailing whitespace)."""

    type: Literal["strip"]
    chars: str | None = None
    per_line: bool = False

    @override
    def process(self, text: str, this_path):
        if self.per_line:
            yield "\n".join(line.strip(self.chars) for line in text.splitlines())
        else:
            yield text.strip(self.chars)


type Operation = ReadFileOperation | ReferenceOperation | ReplaceOperation | RightStripOperation | SplitLinesOperation | StripOperation


class OperationWrapperModel(BaseModel):
    operation: Operation = Field(discriminator="type")


def get_texts(obj: Any, this_path: StrOrPath = ".") -> Iterator[str]:
    source = TextSourceWrapperModel.model_validate({"source": obj}).source
    yield from source.get_texts(this_path)
