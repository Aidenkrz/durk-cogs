import json
from pathlib import Path

from .government import Government

with open(Path(__file__).parent / "info.json", encoding="utf-8") as fp:
    __red_end_user_data_statement__ = json.load(fp)["end_user_data_statement"]


async def setup(bot):
    await bot.add_cog(Government(bot))
