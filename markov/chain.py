"""Markov chain implementation for text generation."""

from __future__ import annotations

import hashlib
import json
import math
import random
import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional, Set


@dataclass
class TokenInfo:
    """Stores information about a token's original form."""
    lowercase: str
    original_forms: Counter = field(default_factory=Counter)

    def add_form(self, form: str) -> None:
        """Record an occurrence of a specific casing."""
        self.original_forms[form] += 1

    def get_most_common_form(self) -> str:
        """Get the most commonly seen casing."""
        if self.original_forms:
            return self.original_forms.most_common(1)[0][0]
        return self.lowercase


class BloomFilter:
    """Simple Bloom filter for fast state existence pre-checks."""

    def __init__(self, size: int = 10000, num_hashes: int = 3):
        self.size = size
        self.num_hashes = num_hashes
        self.bit_array = [False] * size

    def _hashes(self, item: str) -> List[int]:
        """Generate hash values for an item."""
        hashes = []
        for i in range(self.num_hashes):
            h = hashlib.md5(f"{item}{i}".encode()).hexdigest()
            hashes.append(int(h, 16) % self.size)
        return hashes

    def add(self, item: str) -> None:
        """Add an item to the filter."""
        for h in self._hashes(item):
            self.bit_array[h] = True

    def might_contain(self, item: str) -> bool:
        """Check if item might be in the set (can have false positives)."""
        return all(self.bit_array[h] for h in self._hashes(item))


class MarkovChain:
    """An enhanced Markov chain for text generation."""

    START = "__START__"
    END = "__END__"
    PUNCTUATION = {".", "!", "?", ",", ";", ":", "-", "—"}

    def __init__(self, order: int = 2, max_order: int = None):
        """Initialize a Markov chain.

        Args:
            order: The default n-gram order. Default is 2.
            max_order: Maximum order for variable-order chains. If set, trains 1 to max_order.
        """
        self.order = order
        self.max_order = max_order or order

        # Weighted transitions: state -> {word: count}
        self.chain: Dict[Tuple[str, ...], Counter] = {}

        # Reverse chain for bidirectional lookup: state -> {previous_word: count}
        self.reverse_chain: Dict[Tuple[str, ...], Counter] = {}

        # Skip-gram chain: (word1, word3) -> {word2: count} for fill-in
        self.skip_chain: Dict[Tuple[str, str], Counter] = {}

        # Case memory: lowercase -> TokenInfo
        self.case_memory: Dict[str, TokenInfo] = {}

        # Bloom filter for fast lookups
        self.bloom = BloomFilter()

        # Multi-order chains for backoff
        self.order_chains: Dict[int, Dict[Tuple[str, ...], Counter]] = {}
        for i in range(1, self.max_order + 1):
            self.order_chains[i] = {}

    def train(self, text: str) -> None:
        """Add text to the chain with all enhancements.

        Args:
            text: The text to train on.
        """
        tokens = self._tokenize_with_punctuation(text)
        if not tokens:
            return

        # Record case patterns
        for token in tokens:
            lower = token.lower()
            if lower not in self.case_memory:
                self.case_memory[lower] = TokenInfo(lowercase=lower)
            self.case_memory[lower].add_form(token)

        # Normalize to lowercase for chain storage
        words = [t.lower() for t in tokens]

        # Train all order levels (for variable order / backoff)
        for order in range(1, self.max_order + 1):
            padded = [self.START] * order + words + [self.END]

            for i in range(len(padded) - order):
                state = tuple(padded[i : i + order])
                next_word = padded[i + order]

                if state not in self.order_chains[order]:
                    self.order_chains[order][state] = Counter()
                self.order_chains[order][state][next_word] += 1

                # Add to bloom filter
                self.bloom.add(str(state))

                # Primary chain uses default order
                if order == self.order:
                    if state not in self.chain:
                        self.chain[state] = Counter()
                    self.chain[state][next_word] += 1

        # Train reverse chain
        for order in range(1, self.max_order + 1):
            padded = [self.START] * order + words + [self.END]
            for i in range(order, len(padded)):
                state = tuple(padded[i - order + 1 : i + 1])
                prev_word = padded[i - order] if i >= order else self.START

                if order == self.order:
                    if state not in self.reverse_chain:
                        self.reverse_chain[state] = Counter()
                    self.reverse_chain[state][prev_word] += 1

        # Train skip-gram (word1 _ word3 patterns)
        if len(words) >= 3:
            for i in range(len(words) - 2):
                skip_state = (words[i], words[i + 2])
                middle_word = words[i + 1]
                if skip_state not in self.skip_chain:
                    self.skip_chain[skip_state] = Counter()
                self.skip_chain[skip_state][middle_word] += 1

    def generate(
        self,
        min_words: int = 10,
        max_words: int = 50,
        seed: Optional[Tuple[str, ...]] = None,
        temperature: float = 1.0,
    ) -> str:
        """Generate text from the chain.

        Args:
            min_words: Minimum number of words to generate.
            max_words: Maximum number of words to generate.
            seed: Optional starting state.
            temperature: Creativity control (0.1=predictable, 2.0=chaotic). Default 1.0.

        Returns:
            Generated text, or empty string if chain is empty.
        """
        if not self.chain:
            return ""

        result: List[str] = []
        current_order = self.order

        # Use seed or start from beginning
        if seed:
            state = tuple(w.lower() for w in seed)
            if seed[0] != self.START:
                result = list(seed)
        else:
            state = (self.START,) * self.order

        dead_ends = 0
        max_dead_ends = 15

        while len(result) < max_words and dead_ends < max_dead_ends:
            # Try to get next word, with backoff to lower orders
            next_word = self._get_next_word_with_backoff(state, current_order, temperature)

            if next_word is None:
                # Dead end - try to find a continuation
                new_state = self._find_continuation_state(result)
                if new_state:
                    state = new_state
                    dead_ends += 1
                    continue
                else:
                    break

            if next_word == self.END:
                if len(result) < min_words:
                    # Try to continue
                    new_state = self._find_continuation_state(result)
                    if new_state:
                        state = new_state
                        dead_ends += 1
                        continue
                    else:
                        # Try picking a non-END word
                        next_word = self._get_next_word_with_backoff(
                            state, current_order, temperature, exclude={self.END}
                        )
                        if next_word is None:
                            break
                else:
                    break

            # Restore original casing
            display_word = self._restore_case(next_word, result)
            result.append(display_word)

            # Update state
            state = (*state[1:], next_word)

        return self._post_process(result)

    def _get_next_word_with_backoff(
        self,
        state: Tuple[str, ...],
        order: int,
        temperature: float,
        exclude: Set[str] = None,
    ) -> Optional[str]:
        """Get next word with backoff to lower orders if state not found.

        Args:
            state: Current state tuple.
            order: Current order to try.
            temperature: Sampling temperature.
            exclude: Words to exclude from selection.

        Returns:
            Selected word or None if no valid options.
        """
        exclude = exclude or set()

        # Try each order from current down to 1
        for try_order in range(order, 0, -1):
            # Adjust state for this order
            if len(state) >= try_order:
                try_state = state[-try_order:]
            else:
                try_state = state

            chain = self.order_chains.get(try_order, {})
            if try_state in chain:
                counter = chain[try_state]
                # Filter excluded words
                options = {w: c for w, c in counter.items() if w not in exclude}
                if options:
                    return self._weighted_choice(options, temperature)

        return None

    def _weighted_choice(self, counter: Dict[str, int], temperature: float = 1.0) -> str:
        """Select a word based on weighted probability with temperature.

        Args:
            counter: Word -> count mapping.
            temperature: Sampling temperature (lower = more deterministic).

        Returns:
            Selected word.
        """
        if not counter:
            return None

        words = list(counter.keys())
        weights = list(counter.values())

        # Apply temperature
        if temperature != 1.0:
            # Convert to log space, divide by temp, convert back
            weights = [math.pow(w, 1.0 / temperature) for w in weights]

        total = sum(weights)
        r = random.random() * total
        cumulative = 0

        for word, weight in zip(words, weights):
            cumulative += weight
            if r <= cumulative:
                return word

        return words[-1]

    def _restore_case(self, word: str, context: List[str]) -> str:
        """Restore the most appropriate casing for a word.

        Args:
            word: Lowercase word.
            context: Previous words for context.

        Returns:
            Word with restored casing.
        """
        if word in (self.START, self.END):
            return word

        # Check if this should be capitalized (start of sentence)
        should_capitalize = False
        if not context:
            should_capitalize = True
        elif context and context[-1][-1] in ".!?":
            should_capitalize = True

        if word in self.case_memory:
            form = self.case_memory[word].get_most_common_form()
            if should_capitalize and form[0].islower():
                return form[0].upper() + form[1:]
            return form

        if should_capitalize:
            return word.capitalize()
        return word

    def _post_process(self, words: List[str]) -> str:
        """Post-process generated words for proper formatting.

        Args:
            words: List of generated words.

        Returns:
            Formatted string.
        """
        if not words:
            return ""

        result = []
        for i, word in enumerate(words):
            # Handle punctuation spacing
            if word in self.PUNCTUATION:
                if result and not result[-1].endswith(" "):
                    result.append(word)
                else:
                    result.append(word)
            else:
                if result and result[-1] not in self.PUNCTUATION:
                    result.append(" ")
                elif result:
                    result.append(" ")
                result.append(word)

        text = "".join(result).strip()

        # Ensure first letter is capitalized
        if text and text[0].islower():
            text = text[0].upper() + text[1:]

        return text

    def _find_continuation_state(
        self, result: List[str]
    ) -> Optional[Tuple[str, ...]]:
        """Find a state to continue from based on the last words generated."""
        if not result:
            start_state = (self.START,) * self.order
            if start_state in self.chain:
                return start_state
            return None

        # Normalize to lowercase
        result_lower = [w.lower() for w in result]
        last_words = result_lower[-self.order:] if len(result_lower) >= self.order else result_lower

        # Try exact match with last N words
        if len(last_words) == self.order:
            candidate = tuple(last_words)
            if self.bloom.might_contain(str(candidate)) and candidate in self.chain:
                return candidate

        # Try to find any state that starts with the last word
        last_word = result_lower[-1]
        candidates = [s for s in self.chain if s[0] == last_word and s[0] != self.START]
        if candidates:
            return random.choice(candidates)

        # Use reverse chain to find states that could lead here
        if len(last_words) >= 1:
            for s in self.reverse_chain:
                if last_word in s:
                    # Find a forward state that includes this
                    for fwd in self.chain:
                        if fwd[-1] == s[0]:
                            return fwd

        # Last resort: restart from beginning
        start_state = (self.START,) * self.order
        if start_state in self.chain:
            return start_state

        return None

    def find_seed(self, words: List[str]) -> Optional[Tuple[str, ...]]:
        """Find a valid seed state containing the given words."""
        words_lower = [w.lower() for w in words]

        # Try exact match first
        if len(words_lower) >= self.order:
            state = tuple(words_lower[:self.order])
            if state in self.chain:
                return state

        # Search for states containing any of the words
        for word in words_lower:
            for state in self.chain:
                if word in state and self.START not in state:
                    return state

        return None

    def find_middle_word(self, word1: str, word3: str) -> Optional[str]:
        """Use skip-gram to find a word that fits between two others.

        Args:
            word1: First word.
            word3: Third word.

        Returns:
            A word that commonly appears between them, or None.
        """
        skip_state = (word1.lower(), word3.lower())
        if skip_state in self.skip_chain:
            return self._weighted_choice(self.skip_chain[skip_state])
        return None

    def generate_backwards(self, seed: Tuple[str, ...], max_words: int = 10) -> List[str]:
        """Generate words backwards from a seed using the reverse chain.

        Args:
            seed: Starting state.
            max_words: Maximum words to generate.

        Returns:
            List of words in reverse order.
        """
        result = []
        state = tuple(w.lower() for w in seed)

        for _ in range(max_words):
            if state not in self.reverse_chain:
                break
            prev_word = self._weighted_choice(self.reverse_chain[state])
            if prev_word == self.START:
                break
            result.append(prev_word)
            state = (prev_word,) + state[:-1]

        return list(reversed(result))

    def merge_weighted(self, other: "MarkovChain", weight: float = 0.5) -> "MarkovChain":
        """Merge another chain with weighted blending.

        Args:
            other: Chain to merge.
            weight: Weight for other chain (0.0-1.0). 0.5 = equal blend.

        Returns:
            New merged chain.
        """
        merged = MarkovChain(order=self.order, max_order=self.max_order)

        # Merge primary chains
        all_states = set(self.chain.keys()) | set(other.chain.keys())
        for state in all_states:
            merged.chain[state] = Counter()

            if state in self.chain:
                for word, count in self.chain[state].items():
                    merged.chain[state][word] += int(count * (1 - weight))

            if state in other.chain:
                for word, count in other.chain[state].items():
                    merged.chain[state][word] += int(count * weight)

        # Merge case memory (prefer more common forms)
        for lower, info in self.case_memory.items():
            merged.case_memory[lower] = TokenInfo(lowercase=lower)
            merged.case_memory[lower].original_forms.update(info.original_forms)
        for lower, info in other.case_memory.items():
            if lower not in merged.case_memory:
                merged.case_memory[lower] = TokenInfo(lowercase=lower)
            merged.case_memory[lower].original_forms.update(info.original_forms)

        # Merge order chains
        for order in range(1, merged.max_order + 1):
            merged.order_chains[order] = {}
            self_chain = self.order_chains.get(order, {})
            other_chain = other.order_chains.get(order, {})

            all_states = set(self_chain.keys()) | set(other_chain.keys())
            for state in all_states:
                merged.order_chains[order][state] = Counter()
                if state in self_chain:
                    for word, count in self_chain[state].items():
                        merged.order_chains[order][state][word] += int(count * (1 - weight))
                if state in other_chain:
                    for word, count in other_chain[state].items():
                        merged.order_chains[order][state][word] += int(count * weight)

        return merged

    def get_stats(self) -> Dict[str, int]:
        """Get statistics about the chain."""
        transition_count = sum(sum(c.values()) for c in self.chain.values())
        unique_words = len(self.case_memory)
        return {
            "state_count": len(self.chain),
            "transition_count": transition_count,
            "unique_words": unique_words,
            "skip_gram_count": len(self.skip_chain),
        }

    def to_dict(self) -> Dict:
        """Serialize the chain to a dictionary."""
        return {
            "order": self.order,
            "max_order": self.max_order,
            "chain": {json.dumps(list(k)): dict(v) for k, v in self.chain.items()},
            "reverse_chain": {json.dumps(list(k)): dict(v) for k, v in self.reverse_chain.items()},
            "skip_chain": {json.dumps(list(k)): dict(v) for k, v in self.skip_chain.items()},
            "case_memory": {k: dict(v.original_forms) for k, v in self.case_memory.items()},
            "order_chains": {
                str(order): {json.dumps(list(k)): dict(v) for k, v in chain.items()}
                for order, chain in self.order_chains.items()
            },
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "MarkovChain":
        """Deserialize a chain from a dictionary."""
        order = data.get("order", 2)
        max_order = data.get("max_order", order)
        chain = cls(order=order, max_order=max_order)

        chain.chain = {
            tuple(json.loads(k)): Counter(v)
            for k, v in data.get("chain", {}).items()
        }
        chain.reverse_chain = {
            tuple(json.loads(k)): Counter(v)
            for k, v in data.get("reverse_chain", {}).items()
        }
        chain.skip_chain = {
            tuple(json.loads(k)): Counter(v)
            for k, v in data.get("skip_chain", {}).items()
        }

        # Restore case memory
        for lower, forms in data.get("case_memory", {}).items():
            chain.case_memory[lower] = TokenInfo(lowercase=lower)
            chain.case_memory[lower].original_forms = Counter(forms)

        # Restore order chains
        for order_str, order_data in data.get("order_chains", {}).items():
            order_int = int(order_str)
            chain.order_chains[order_int] = {
                tuple(json.loads(k)): Counter(v)
                for k, v in order_data.items()
            }

        # Rebuild bloom filter
        for state in chain.chain:
            chain.bloom.add(str(state))

        return chain

    def _tokenize_with_punctuation(self, text: str) -> List[str]:
        """Tokenize text while preserving punctuation as separate tokens.

        Args:
            text: The text to tokenize.

        Returns:
            List of tokens.
        """
        # Split punctuation from words but keep it
        # "Hello, world!" -> ["Hello", ",", "world", "!"]
        tokens = []
        current_word = []

        for char in text:
            if char.isspace():
                if current_word:
                    tokens.append("".join(current_word))
                    current_word = []
            elif char in self.PUNCTUATION:
                if current_word:
                    tokens.append("".join(current_word))
                    current_word = []
                tokens.append(char)
            else:
                current_word.append(char)

        if current_word:
            tokens.append("".join(current_word))

        return [t for t in tokens if t]

    def _tokenize(self, text: str) -> List[str]:
        """Legacy tokenize method for compatibility."""
        return self._tokenize_with_punctuation(text)


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
