import io
import struct
import os
import time

# Source disk to read from
SOURCE_DISK = "/dev/sdb"
# Target location to save recovered files to
TARGET_LOCATION = "/media/home/backup"


class DiskReader:
    chunk_size = 1024 * 1024 * 5

    def __init__(self, disk: io.BytesIO):
        self.disk = disk
        self.current_offset = 0
        self.buffer = b""
        self.total_bytes_read = 0
        self.reading_buffer = False

    def __repr__(self) -> str:
        nice_size = self.total_bytes_read
        if self.total_bytes_read > 1024 * 1024 * 1024:
            nice_size = f"{self.total_bytes_read / 1024 / 1024 / 1024:.2f} GB"
        else:
            nice_size = f"{self.total_bytes_read / 1024 / 1024:.2f} MB"
        return f"<DiskReader {nice_size} {self.current_offset}/{len(self.buffer)}>"

    def __iter__(self):
        return self

    def __next__(self) -> bytes:
        if self.current_offset >= len(self.buffer):
            self.buffer = self.disk.read(self.chunk_size)
            self.current_offset = 0
        if len(self.buffer) == 0:
            raise StopIteration
        val = self.buffer[self.current_offset : self.current_offset + 1]
        self.current_offset += 1
        self.total_bytes_read += 1
        return val

    def read_all(self) -> bytes:
        while True:
            if self.current_offset >= len(self.buffer):
                self.buffer = self.disk.read(self.chunk_size)
                self.current_offset = 0
            if len(self.buffer) == 0:
                return
            self.reading_buffer = True
            # for byte in self.buffer[self.current_offset :]:
            chunk = self.buffer[self.current_offset :]
            for byte in struct.unpack(str(len(chunk)) + "c", chunk):
                self.current_offset += 1
                self.total_bytes_read += 1
                yield byte
                # yield bytes([byte])
                if not self.reading_buffer:
                    break

    def peek(self, size: int) -> bytes:
        if self.current_offset + size > len(self.buffer):
            chunk = self.disk.read(size)
            self.buffer += chunk
        return self.buffer[self.current_offset : self.current_offset + size]

    def read(self, size: int) -> bytes:
        self.reading_buffer = False
        if self.current_offset + size > len(self.buffer):
            chunk = self.disk.read(size)
            self.buffer += chunk
        val = self.buffer[self.current_offset : self.current_offset + size]
        self.current_offset += size
        self.total_bytes_read += size
        return val


START_STREAM_MATCH = b"\x00\x00\x00\x08wide***\xf8mdat"

_start = time.time()


def log(msg: str, nl=True) -> None:
    prefix = f'[{"%.2f" % (time.time() - _start)}] '
    if nl:
        print(prefix + msg)
    else:
        print(prefix + msg, end="\r")


def run():
    last6 = b""
    found_file_num = 0
    current_file = None
    current_match_pos = 0
    match_buffer = b""
    with open(SOURCE_DISK, "rb") as disk:
        fi_reader = DiskReader(disk)

        # for byte in fi_reader:
        for byte in fi_reader.read_all():
            if fi_reader.total_bytes_read % 100000 == 0:
                log(f"Reading {fi_reader}", nl=False)

            if current_file is not None:
                last6 = last6[-5:] + byte
                if last6[0:1] == b"\x00" and last6.endswith(b"data"):
                    datum_size = last6[1]
                    datum = fi_reader.read(datum_size + 3)
                    log(f"Found datum: {datum_size} {len(datum)} {datum}")
                    current_file.write(byte + datum)
                    if not fi_reader.peek(5).endswith(b"data"):
                        log(f"End of file found: {current_file}")
                        current_file.close()
                        current_file = None
                else:
                    current_file.write(byte)
            elif (
                byte == START_STREAM_MATCH[current_match_pos : current_match_pos + 1]
                or START_STREAM_MATCH[current_match_pos : current_match_pos + 1] == b"*"
            ):
                current_match_pos += 1
                match_buffer += byte
                if current_match_pos == len(START_STREAM_MATCH):
                    found_file_num += 1
                    current_match_pos = 0
                    filename = f"found-file-{found_file_num}.braw"
                    current_file = open(os.path.join(TARGET_LOCATION, filename), "wb")
                    current_file.write(match_buffer)
                    log(f"Found file! starting write out: {current_file}")
                    match_buffer = b""
            else:
                current_match_pos = 0
                match_buffer = b""


if __name__ == "__main__":
    run()
