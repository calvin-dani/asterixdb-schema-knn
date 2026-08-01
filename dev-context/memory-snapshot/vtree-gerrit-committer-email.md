---
name: vtree-gerrit-committer-email
description: AsterixDB Gerrit pushes as Le0shy are rejected if the commit COMMITTER email is couchbase (must be leoshy1005@gmail.com); reconstructed stacks pick up the wrong committer
metadata: 
  node_type: memory
  type: reference
  originSessionId: 7215a24d-5931-4f2d-a7f7-01a58dabdad1
---

The AsterixDB ASF Gerrit remote is `ssh://Le0shy@asterix-gerrit.ics.uci.edu:29418/asterixdb`. Only `leoshy1005@gmail.com` is registered on that account. A push is rejected (`email address hongyu.shi@couchbase.com is not registered ... lack 'forge committer' permission`) when a commit's **committer** email is the couchbase one.

The local git config here is `user.email=hongyu.shi@couchbase.com`, so any cherry-pick / amend / rebase during a stack reconstruction stamps that as the committer and breaks the push.

**Fix before pushing:** force-rebase the whole stack overriding the committer:
`GIT_COMMITTER_NAME="Le0shy" GIT_COMMITTER_EMAIL="leoshy1005@gmail.com" git rebase -f <base> <branch>`
This changes committer only; authors are preserved (Le0shy on 3754 p1/p2/p3 and 3771; Calvin `calvinthomas.dani@gmail.com` on 3760 and spann). Verify with `git log --format='%ae %ce'` — want zero `couchbase` and only 3771 authored by leoshy among 3760/3771/spann.

Direct push (Le0shy-owned changes): `git push gerrit <p3-tip>:refs/for/master` — uploads p1/p2/p3 as new patchsets on 21099/21100/21101 (base 3d6992d0e7=3702 is merged to master, so only those 3 go up). 3760/3771/spann need the admin forge-committer bundle upload (see [[vtree-review-fixes-patchset-mapping]]). Changing the committer changes SHAs, so rebuild the bundle after.
