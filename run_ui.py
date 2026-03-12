from __future__ import annotations

import uvicorn

from script_runner.app import APP_HOST, APP_PORT


def main() -> None:
    uvicorn.run(
        "script_runner.app:app",
        host=APP_HOST,
        port=APP_PORT,
        workers=1,
        reload=False,
    )


if __name__ == "__main__":
    main()

