# onlyfans-dl

## Setup

1. Clone the repo
2. Run `poetry shell`
3. Run `poetry install`
4. Run `python -m onlyfans_dl`

On initial startup, you will be asked for the `cookie`, `user_agent`, and `x_bc` values.
After inputting, you will be asked if you would like to begin scraping.
If you want to edit your configuration (see options below), input `n` and edit the file outputted.

- `download_root` - Output directory for downloaded files.
Files are organized by creator.
Defaults to `downloads`.
- `proxy` - Address of a proxy to use for scraping.
Examples: `http://192.168.10.10:8080`, `socks5h://192.168.10.10:1080`.
- `skip_temporary` - Whether or not to skip downloading temporary posts (expiring posts and stories).
Useful for making it less obvious when you were subscribed to a creator.
Defaults to `false`.

Get these values from the browser developer tools:
- `cookie`
- `user_agent`
- `x_bc`
