"""Markov chain text generation cog."""

from pathlib import Path
import json

from .markov import Markov

with open(Path(__file__).parent / "info.json") as fp:
    __red_end_user_data_statement__ = json.load(fp)["end_user_data_statement"]


async def setup(bot):
    """Load the Markov cog."""
    await bot.add_cog(Markov(bot))
