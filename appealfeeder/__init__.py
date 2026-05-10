from .appealfeeder import AppealFeeder


async def setup(bot):
    await bot.add_cog(AppealFeeder(bot))
