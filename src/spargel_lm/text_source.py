import gzip
import logging
from pathlib import Path
from typing import Any, Iterator, Literal, override

from pydantic import BaseModel, Field

from .typing import StrOrPath

logger = logging.getLogger(__name__)


def _resolve_parent(path: StrOrPath):
    return Path(path).resolve().parent


#### Text sources ####


class TextSourceModel(BaseModel):
    """Base class for text source models"""

    def get_texts(self, this_path: StrOrPath) -> Iterator[str]: ...


class PlainTextSource(TextSourceModel):
    """Provide a list of plain texts directly."""

    type: Literal["text"]
    texts: list[str]

    @override
    def get_texts(self, this_path):
        yield from self.texts


class ProcessSource(TextSourceModel):
    """Process texts."""

    type: Literal["process"]
    operations: list[Operation]
    sources: list[TextSource]

    @override
    def get_texts(self, this_path):
        for source in self.sources:
            texts = list(source.get_texts(this_path))
            for operation in self.operations:
                tmp: list[str] = []
                for text in texts:
                    tmp.extend(operation.process(text, this_path))
                texts = tmp

            yield from texts


type TextSource = PlainTextSource | ProcessSource


class TextSourceWrapperModel(BaseModel):
    source: TextSource = Field(discriminator="type")


#### Operations on texts ####


class OperationModel(BaseModel):
    """Base class for text operation models"""

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


class RightStripOperation(OperationModel):
    """Right-strip text (remove trailing whitespace)."""

    type: Literal["rstrip"]
    chars: str | None = None

    @override
    def process(self, text: str, this_path):
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

    @override
    def process(self, text: str, this_path):
        yield text.strip(self.chars)


type Operation = ReadFileOperation | RightStripOperation | SplitLinesOperation | StripOperation


class OperationWrapperModel(BaseModel):
    operation: Operation = Field(discriminator="type")


def get_texts(obj: Any, this_path: StrOrPath = ".") -> Iterator[str]:
    source = TextSourceWrapperModel.model_validate({"source": obj}).source
    yield from source.get_texts(this_path)
