# Government

`Government` is a Red cog for persistent political parties, elections, terms,
succession, and constitutional lawmaking. It uses this repository's `Polls` cog
for persistent button voting.

## Setup

1. Load `Polls`, then load `Government`.
2. Run `[p]government admin setup #channel`. The cog also creates a read-only
   `current-laws` channel, publishes the five immutable provisions, seeds the
   six Presidency rules as separate amendable founding laws, and immediately
   backfills every enacted law already in its stored register. Use `[p]government
   admin set-laws-channel` if you prefer an existing channel.
3. Put the bot's role above the roles it creates and grant it **Manage Roles**,
   **Manage Channels**, **Manage Messages**, **Send Messages**, **Embed Links**,
   **Add Reactions**, and **Read Message History**. **Create Public Threads** is
   optional but recommended for law discussion threads.
4. Configure a campaign channel so the permissionless `Party Leader` role can
   post there. Only leaders of parties with at least five current members keep
   that shared role.
5. Run `[p]government admin set-party-category` to select where private party
   channels belong. A party receives a private text channel when it reaches five
   members. The channel grants access only through that party's role; Discord
   administrators and the server owner retain their normal override access.

## Lifecycle

- A party founder becomes its leader and first member. Members can belong to
  only one party. PNG, JPEG, and static WebP icons are signature-checked and
  limited to 256 KiB. Leaders may transfer leadership to another party member
  while their party is not on an active ballot.
- `[p]government party info <party>` shows a party's saved icon, exact color, leader,
  slogan, description, manifesto, founding date, qualifying role status, and
  current member list.
- Party leaders can update those fields with `/government party edit`; entering
  `-` clears a slogan, description, or manifesto. Profiles intentionally have
  no website field or public application button.
- A leader can propose a merger with `[p]government party merge propose @OtherLeader New Party Name`.
  The invited leader has 24 hours to accept. Once accepted, memberships are
  combined into a newly named party led by the proposer. It inherits the
  proposer's profile, color, and icon; the old roles and channels are replaced
  with the merged party's managed resources.
- At five members, the cog creates a party role with the requested color and
  icon and no permissions. The leader also receives the shared, permissionless
  `Party Leader` role.
- At five members, the cog creates a private party text channel in the configured
  category. Membership changes automatically control access through the party
  role, and unauthorized manual role assignments are removed.
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
- `[p]government law repeal` lets the President propose removing an enacted law.
  Repeals use the same discussion and voting requirements as the target law;
  once passed, the old law is removed from `current-laws` while its historical
  record is retained.
- `[p]government law amend` proposes replacement text for an enacted law. A
  successful vote archives the old version as amended and publishes the new
  version. The six founding government rules require two-thirds votes to amend
  or repeal; the five Immutable Laws are not law records and cannot be targeted.
- Administrators can void proposals or enacted laws that conflict with the
  immutable Discord, legal, safety, owner-authority, or infrastructure rules.

Use `[p]government admin reconcile` after manually deleting or moving roles, or
after correcting a role hierarchy or channel-permission problem. Administrators
can also use `[p]government admin rename-party` and `[p]government admin
delete-party` to manage party records and their associated roles/channels.

## Command style

`[p]` means your server's configured Red prefix. Most Government commands are
prefix commands and can also be reached through the shorter `[p]gov` alias.
Only `/government party create` and `/government party edit` remain slash
commands because they use Discord attachment inputs and several optional profile
fields.

Useful prefix examples:

- `[p]government party list`
- `[p]government party join Party Name`
- `[p]government party merge propose @OtherLeader New Party Name`
- `[p]government party merge accept abc123`
- `[p]government law propose ordinary "Park Rules" Be respectful in the park.`
- `[p]government law amend 3 "Updated Park Rules" Replacement law text here.`
- `[p]government admin rename-party "Old Party" New Party`
- `[p]government admin delete-party Party Name`

The party list uses one message with reaction controls to move between pages.
