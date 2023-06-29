#! /bin/bash

git filter-branch --env-filter '

OLD_EMAIL="pveinberg@gmail.com"
CORRECT_NAME="Pablo Veinberg - Minsait"
CORRECT_EMAIL="psebastianv@minsait.com"

if [ "$GIT_COMMITTER_EMAIL" = "$OLD_EMAIL" ]
then
    export GIT_COMMITTER_NAME="$CORRECT_NAME"
    export GIT_COMMITTER_EMAIL="$CORRECT_EMAIL"
fi
if [ "$GIT_AUTHOR_EMAIL" = "$OLD_EMAIL" ]
then
    export GIT_AUTHOR_NAME="$CORRECT_NAME"
    export GIT_AUTHOR_EMAIL="$CORRECT_EMAIL"
fi
' --tag-name-filter cat -- --branches --tags

# Outros problemas durante o merge da branch develop com a mian 

# git pull origin main --allow-unrelated-histories
# git reset HEAD~ .
# git stash .
# git pop . 
# git stash apply