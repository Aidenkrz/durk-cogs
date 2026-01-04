"""Markov chain implementation for text generation."""

from __future__ import annotations

import json
import random
import re
from typing import Dict, List, Tuple, Optional


class MarkovChain:
    """A Markov chain for text generation."""

    START = "__START__"
    END = "__END__"

    def __init__(self, order: int = 2):
        """Initialize a Markov chain.

        Args:
            order: The n-gram order (number of words in state). Default is 2.
        """
        self.order = order
        self.chain: Dict[Tuple[str, ...], List[str]] = {}

    def train(self, text: str) -> None:
        """Add text to the chain.

        Args:
            text: The text to train on.
        """
        words = self._tokenize(text)
        if not words:
            return

        # Add start and end markers
        padded = [self.START] * self.order + words + [self.END]

        for i in range(len(padded) - self.order):
            state = tuple(padded[i : i + self.order])
            next_word = padded[i + self.order]
            if state not in self.chain:
                self.chain[state] = []
            self.chain[state].append(next_word)

    def generate(
        self,
        min_words: int = 10,
        max_words: int = 50,
        seed: Optional[Tuple[str, ...]] = None,
    ) -> str:
        """Generate text from the chain.

        Args:
            min_words: Minimum number of words to generate.
            max_words: Maximum number of words to generate.
            seed: Optional starting state. If provided, must match the chain order.

        Returns:
            Generated text, or empty string if chain is empty.
        """
        if not self.chain:
            return ""

        result: List[str] = []

        # Use seed or start from beginning
        if seed:
            state = seed
            if seed[0] != self.START:
                result = list(seed)
        else:
            state = (self.START,) * self.order

        dead_ends = 0
        max_dead_ends = 10  # Prevent infinite loops

        while len(result) < max_words and dead_ends < max_dead_ends:
            if state not in self.chain:
                # Dead end - try to find a new state using last word(s)
                new_state = self._find_continuation_state(result)
                if new_state:
                    state = new_state
                    dead_ends += 1
                    continue
                else:
                    break

            next_word = random.choice(self.chain[state])

            if next_word == self.END:
                # If we haven't reached min_words, try to continue
                if len(result) < min_words:
                    # Try to find a state using the last word(s) to continue
                    new_state = self._find_continuation_state(result)
                    if new_state:
                        state = new_state
                        dead_ends += 1
                        continue
                    else:
                        # No continuation found, try picking a non-END word
                        alternatives = [w for w in self.chain[state] if w != self.END]
                        if alternatives:
                            next_word = random.choice(alternatives)
                        else:
                            break
                else:
                    break

            result.append(next_word)
            state = (*state[1:], next_word)

        return " ".join(result)

    def _find_continuation_state(
        self, result: List[str]
    ) -> Optional[Tuple[str, ...]]:
        """Find a state to continue from based on the last words generated.

        Args:
            result: The words generated so far.

        Returns:
            A valid state tuple to continue from, or None.
        """
        if not result:
            # No words yet, start fresh
            start_state = (self.START,) * self.order
            if start_state in self.chain:
                return start_state
            return None

        # Try to find a state that starts with the last word(s)
        last_words = result[-self.order :] if len(result) >= self.order else result

        # First, try exact match with last N words
        if len(last_words) == self.order:
            candidate = tuple(last_words)
            if candidate in self.chain:
                return candidate

        # Try to find any state that starts with the last word
        last_word = result[-1]
        candidates = [s for s in self.chain if s[0] == last_word and s[0] != self.START]
        if candidates:
            return random.choice(candidates)

        # Last resort: find any state containing the last word
        candidates = [
            s for s in self.chain
            if last_word in s and self.START not in s and self.END not in s
        ]
        if candidates:
            return random.choice(candidates)

        # Nothing found, restart from beginning
        start_state = (self.START,) * self.order
        if start_state in self.chain:
            return start_state

        return None

    def find_seed(self, words: List[str]) -> Optional[Tuple[str, ...]]:
        """Find a valid seed state containing the given words.

        Args:
            words: Words to search for in the chain.

        Returns:
            A valid state tuple, or None if not found.
        """
        # Try to find an exact match first
        if len(words) >= self.order:
            state = tuple(words[: self.order])
            if state in self.chain:
                return state

        # Search for states containing any of the words
        for word in words:
            for state in self.chain:
                if word.lower() in [w.lower() for w in state if w != self.START]:
                    return state

        return None

    def merge(self, other: "MarkovChain") -> None:
        """Merge another chain into this one.

        Args:
            other: The chain to merge.
        """
        for state, transitions in other.chain.items():
            if state not in self.chain:
                self.chain[state] = []
            self.chain[state].extend(transitions)

    def get_stats(self) -> Dict[str, int]:
        """Get statistics about the chain.

        Returns:
            Dictionary with state_count and transition_count.
        """
        transition_count = sum(len(t) for t in self.chain.values())
        return {
            "state_count": len(self.chain),
            "transition_count": transition_count,
        }

    def to_dict(self) -> Dict:
        """Serialize the chain to a dictionary.

        Returns:
            Dictionary representation of the chain.
        """
        return {
            "order": self.order,
            "chain": {json.dumps(list(k)): v for k, v in self.chain.items()},
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "MarkovChain":
        """Deserialize a chain from a dictionary.

        Args:
            data: Dictionary representation of the chain.

        Returns:
            MarkovChain instance.
        """
        chain = cls(order=data.get("order", 2))
        chain.chain = {
            tuple(json.loads(k)): v for k, v in data.get("chain", {}).items()
        }
        return chain

    def _tokenize(self, text: str) -> List[str]:
        """Clean and split text into words.

        Args:
            text: The text to tokenize.

        Returns:
            List of words.
        """
        # Split on whitespace and filter empty strings
        words = text.split()
        return [w for w in words if w]


def sanitize_message(text: str) -> str:
    """Sanitize a Discord message for training.

    Removes/replaces mentions, URLs, and custom emojis to avoid
    pinging users when generating text.

    Args:
        text: The raw message content.

    Returns:
        Sanitized text.
    """
    # Replace user mentions with placeholder
    text = re.sub(r"<@!?\d+>", "[user]", text)
    # Replace role mentions
    text = re.sub(r"<@&\d+>", "[role]", text)
    # Replace channel mentions
    text = re.sub(r"<#\d+>", "[channel]", text)
    # Remove URLs
    text = re.sub(r"https?://\S+", "", text)
    # Remove custom emojis (keep unicode emojis)
    text = re.sub(r"<a?:\w+:\d+>", "", text)
    # Normalize whitespace
    text = " ".join(text.split())
    return text.strip()
