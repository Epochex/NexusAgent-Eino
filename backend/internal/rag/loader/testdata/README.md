# Dedicated parser resource fixture

`pdf-literal-768m.pdf.gz` is a generated 1,844-byte gzip containing a
783,342-byte PDF. Its single FlateDecode content stream contains a 768 MiB
literal string of `X` characters inside `BT /F1 12 Tf 10 20 Td (...) Tj ET`.
The page uses Helvetica and a normal xref/trailer. No external document or
copyrighted source was used.

The resource test expands only the outer gzip locally, with a 32 MiB bound.
It sends the PDF exclusively to the separately constrained parser container.
The inner string intentionally exceeds that container's hard memory limit.
Ordinary unit tests never parse this fixture in their own process.

Keeping the fixed compressed sample avoids spending the race-test request
deadline generating 768 MiB of input. The parser request still receives the
entire hostile PDF; its size and recovery outcome are logged.
