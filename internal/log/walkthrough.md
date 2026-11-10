one simple record: `"CAT"`.

Assume the segment has `baseOffset = 5`.

## 1. The `.store` file

Your Store writes every record in this format:

```
[8 bytes: payload length][payload bytes]
```

For `"CAT"`:

- Payload length = 3 bytes.
- Payload = `C`, `A`, `T`.

The `.store` file looks like this:

5.store

# 8

Length prefix

# CAT

Payload

Bytes 0–7Bytes 8–10

The length prefix contains the integer 3 encoded as an 8-byte big-endian uint64.

The actual bytes in hexadecimal are:

```
00 00 00 00 00 00 00 03  43 41 54
|-------- length -------|  | CAT |
```

The next record starts at byte position 11.

## 2. The `.index` file

Your Index stores:

```
[4 bytes: relative offset][8 bytes: Store position]
```

For our record:

```
baseOffset       = 5
logical offset   = 5
relative offset  = 5 - 5 = 0
Store position   = 0
```

Therefore, the `.index` file contains:

5.index

# 0

Relative offset

# 0

Store position

Bytes 0–3Bytes 4–11

One Index entry = 4 + 8 = 12 bytes.

In hexadecimal:

```
00 00 00 00  00 00 00 00 00 00 00 00
|-- off --|  |------ store pos ------|
```

## 3. Now append another record: "DOG"

Suppose `"DOG"` also takes 11 bytes in the Store.

Store file:

```
5.store

Position 0:
[8-byte length = 3][CAT]

Position 11:
[8-byte length = 3][DOG]
```

Index file:

```
5.index

Entry 0:
[4-byte relative offset = 0]
[8-byte Store position = 0]

Entry 1:
[4-byte relative offset = 1]
[8-byte Store position = 11]
```

| Record | Logical offset | Relative offset | Store position |
| ------ | -------------- | --------------- | -------------- |
| CAT    | 5              | 0               | 0              |
| DOG    | 6              | 1               | 11             |

## 4. How reading DOG works

You call:

```
s.Read(6)
```

The Segment calculates:

```
relativeOffset := 6 - 5 // 1
```

Index reads entry 1:

```
relative offset = 1
Store position  = 11
```

Then Store reads from byte position 11:

```
[8-byte length = 3][DOG]
```

And returns `"DOG"`.

Remember these two file formats:

Store

```
[8-byte length][actual record bytes]
```

Index

```
[4-byte relative offset][8-byte Store position]
```

That's it! The `.store` file contains the actual data; the `.index` file tells Scribe exactly where to find it.