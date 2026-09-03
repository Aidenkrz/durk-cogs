# Government

`Government` is a Red cog for persistent political parties, elections, terms,
succession, and constitutional lawmaking. It uses this repository's `Polls` cog
for persistent button voting.

## Setup

1. Load `Polls`, then load `Government`.
2. Run `/government admin setup #channel`. The cog also creates a read-only
   `current-laws` channel, publishes the Founding Constitution, and immediately
   backfills every enacted law already in its stored register. Use `/government
   admin set-laws-channel` if you prefer an existing channel.
3. Put the bot's role above the roles it creates and grant it **Manage Roles**,
   **Manage Channels**, **Manage Messages**, **Send Messages**, **Embed Links**,
   and **Create Public Threads** (the last is optional but recommended for law
   discussion threads).
4. Configure a campaign channel so the permissionless `Party Leader` role can
   post there. Only leaders of parties with at least five current members keep
   that shared role.

## Lifecycle

- A party founder becomes its leader and first member. Members can belong to
  only one party. PNG, JPEG, and static WebP icons are signature-checked and
  limited to 256 KiB. Leaders may transfer leadership to another party member
  while their party is not on an active ballot.
- At five members, the cog creates a party role with the requested color and
  icon and no permissions. The leader also receives the shared, permissionless
  `Party Leader` role.
- An administrator can start an election when no election is open and the
  current term has at most 24 hours remaining. All qualifying parties appear on
  the secret ballot. A unique plurality elects the party leader for 14 days;
  ties require a new election.
- The President appoints a Vice President. If the President leaves the server
  or an administrator vacates the office, the Vice President serves the rest of
  the existing term.
- A presidential law proposal receives 24 hours of public discussion, then an
  automatic 24-hour public vote. Ordinary laws need more approvals than
  rejections; constitutional amendments need at least two-thirds approval.
- `/government law repeal` lets the President propose removing an enacted law.
  Repeals use the same discussion and voting requirements as the target law;
  once passed, the old law is removed from `current-laws` while its historical
  record is retained.
- Administrators can void proposals or enacted laws that conflict with the
  immutable Discord, legal, safety, owner-authority, or infrastructure rules.

Use `/government admin reconcile` after manually deleting or moving roles, or
after correcting a role hierarchy problem.
