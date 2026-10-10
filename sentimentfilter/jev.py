"""TypeSafe Jev 1.13 through OpenRouter's System One API.

Jev answers typed questions about a piece of text with calibrated
probabilities in one pass. Each message is asked three questions in a single
request: whether it breaks the configured rule, how severe it is, and whether
its tone is positive (used for SocialCredit rewards).
"""

import asyncio
from dataclasses import dataclass
from typing import Optional

import aiohttp

ENDPOINT = "https://openrouter.ai/api/v1/systemone"
MODEL = "typesafe/jev-1.13"
MAX_STATE_CHARS = 4000

DEFAULT_QUESTION = "Does this Discord message break the server's no-negativity rule?"
DEFAULT_FLAG = (
    "Hostility toward a person (insults, threats, harassment, hate, telling someone to hurt "
    "themselves - including disguised spellings or sarcasm), or complaining: whining or "
    "negativity about something with no constructive point."
)
DEFAULT_ALLOW = (
    "Constructive criticism: specific, respectful feedback that explains what is wrong or "
    "suggests an improvement. Complaints are not constructive criticism. Also allowed: friendly "
    "banter, self-deprecating jokes, sharing sad news or personal struggles, and quoting or "
    "reporting what someone else said."
)
SEVERITY_LEVELS = ["none", "mild", "moderate", "severe", "extreme"]


class JevError(Exception):
    """The request failed or returned something unexpected."""


@dataclass(frozen=True)
class Judgement:
    flagged_p: float  # p(yes) for the configured question
    severity: float  # expected level, 0 (none) to 4 (extreme)
    positive: float  # p(tone is positive or friendly)
    cost: Optional[float]

    @property
    def severity_label(self) -> str:
        return SEVERITY_LEVELS[min(len(SEVERITY_LEVELS) - 1, round(self.severity))]


class JevClient:
    def __init__(self, api_key: str):
        self._session = aiohttp.ClientSession(
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=aiohttp.ClientTimeout(total=10),
        )

    async def close(self) -> None:
        await self._session.close()

    async def judge(self, text: str, *, question: str, flag: str, allow: str) -> Judgement:
        payload = {
            "model": MODEL,
            "state": text[:MAX_STATE_CHARS],
            "questions": {
                "flagged": {
                    "type": "noul",
                    "instructions": question,
                    "criteria": {"true": flag, "false": allow},
                },
                "severity": {
                    "type": "score",
                    "instructions": "How severe is the negativity or hostility in this message?",
                    "criteria": SEVERITY_LEVELS,
                },
                "positive": {
                    "type": "noul",
                    "instructions": "Is the tone of this message positive or friendly?",
                },
            },
        }
        try:
            async with self._session.post(ENDPOINT, json=payload) as resp:
                status = resp.status
                body = await resp.json(content_type=None)
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
            raise JevError(f"{type(exc).__name__}: {exc}") from exc

        if status != 200:
            error = body.get("error", body) if isinstance(body, dict) else body
            message = error.get("message", error) if isinstance(error, dict) else error
            raise JevError(f"HTTP {status}: {str(message)[:200]}")
        try:
            answers = body["answers"]
            return Judgement(
                flagged_p=float(answers["flagged"]["noul"]),
                severity=float(answers["severity"]["score"]),
                positive=float(answers["positive"]["noul"]),
                cost=(body.get("usage") or {}).get("cost"),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise JevError(f"Unexpected response: {str(body)[:200]}") from exc
