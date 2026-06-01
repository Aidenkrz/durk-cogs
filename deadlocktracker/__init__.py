from .deadlocktracker import DeadlockTracker


async def setup(bot):
    await bot.add_cog(DeadlockTracker(bot))
