# box - Command Line Tool

The `box` command line tool for creating, extracting, and managing `.box` archives.

## Installation

```bash
cargo install --path cli
```

## Commands

### Create an archive

```bash
box create archive.box file1.txt file2.txt directory/
```

### Extract an archive

```bash
box extract archive.box -o output_directory/
```

### List contents

```bash
box list archive.box
```

### Show archive info

```bash
box info archive.box
```

### Validate integrity

```bash
box validate archive.box
```

## Screenshots

### Unix

<img width="809" src="https://user-images.githubusercontent.com/279099/92532977-d17a6480-f231-11ea-8d8c-637918d2c6cc.png">

### Windows

<img width="815" src="https://user-images.githubusercontent.com/279099/92532802-6466cf00-f231-11ea-9fc1-4e9342b37dd3.png">

## License

Licensed under the European Union Public License, version 1.2 ([LICENSE](LICENSE) or https://joinup.ec.europa.eu/collection/eupl/eupl-text-11-12)

The EUPL is a copyleft, GPL-compatible license. See [this introduction](https://joinup.ec.europa.eu/collection/eupl/introduction-eupl-licence) for more information.
