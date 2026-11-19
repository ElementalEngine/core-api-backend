# Privacy Policy

**Applies to:** Authbot, Lady Justice, and Mito Bot — the Discord applications operated for the Civilization Players League (CPL) by ElementalEngine.

**Last updated:** 06/09/2026

**Contact:** civplayersleague@ElementalEngine.onmicrosoft.com

---

## Summary

These bots run inside the CPL Discord server. To do their jobs they store some information about members on servers we control, outside of Discord.

**None of the three bots read, store, or process the content of your messages.** They work through slash commands, buttons, and menus only. We do not track your presence or online status.

---

## What we collect

### Authbot — account verification

When you register, we store:

- Your Discord user ID, username, and display name
- Your Discord locale, whether your Discord account is email-verified, and whether you have two-factor authentication enabled — recorded once, at the moment you register
- The gaming account you link: platform (Steam, Epic, 2K, or Xbox), account ID, and account name
- For Steam registrations: your recorded playtime in Civilization VI and/or VII, and the time we verified you own the game
- Which games you are registered for, and how each registration was verified

Registration also creates a temporary session record holding a random session identifier and a one-time token. These are valid for minutes, not days, and expire automatically.

### Lady Justice — moderation

When a moderator issues a warning or suspension, we store:

- Your Discord user ID
- The infraction category or type, the tier reached, and the number of days applied
- The suspension start and end dates
- Any reason text the moderator entered
- The Discord role IDs you held at the time of suspension — stored so your roles can be restored automatically when the suspension ends

### Mito Bot — match reporting and ranked stats

When a match is reported, we store:

- The Discord user IDs of the players involved
- The match result and the game settings agreed for that match
- Your TrueSkill rating and your season and lifetime statistics

Match reports are submitted by uploading a Civilization save file. The file is read to determine the outcome of the game, and the result of that reading is what we keep.

---

## Why we collect it

- **To verify eligibility.** Confirming you own the game and have enough playtime is what gates access to ranked play.
- **To manage roles.** All three bots add and remove Discord roles — verification roles, rank roles, and suspension roles.
- **To run the league.** Ratings, leaderboards, and match history require a persistent record of results.
- **To enforce the rules.** Infraction history is what makes escalating penalties possible, and storing your roles is what allows them to be given back.

We do not sell your data, share it with advertisers, or use it to train machine learning or AI models.

---

## Where it is stored

On MongoDB Atlas, reached only through our own backend service. Access is limited to the project maintainers.

## How long we keep it

We keep your records for as long as the league operates, unless you ask us to delete them.

- **Verification records** persist so you do not have to re-verify. They remain if you leave the Discord server, so that returning members keep their verified status.
- **Match results and ratings** are part of the league's competitive record and are kept as historical results.
- **Infraction and suspension history** is retained so that penalties escalate correctly over time.
- **Registration sessions** expire automatically within minutes and are not kept.

## Third parties

- **Discord** — we receive your account information through Discord's API. Discord's own privacy policy governs your relationship with them.
- **Steam (Valve)** — for Steam registrations we query the Steam Web API to confirm game ownership and playtime. We send your Steam ID; we do not send Valve anything about your activity with us.

## Your choices

- **Access** — you can ask what we hold about you.
- **Deletion** — you can ask us to delete your verification record and the gaming account linked to it. Moderation history may be retained where it is needed for the integrity of the league; if we decline a deletion on that basis we will tell you why.
- **Correction** — if a linked account or a match result is wrong, tell us and we will fix it.
- **Withdrawing** — leaving the CPL Discord server stops any further collection.

To make a request, use the contact route above.

## Changes

If this policy changes materially we will post a notice in the CPL Discord server. The date at the top always reflects the current version.
