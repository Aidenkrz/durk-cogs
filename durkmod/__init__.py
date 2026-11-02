from .durkmod import DurkMod

async def setup(bot):
    await bot.add_cog(DurkMod(bot))
