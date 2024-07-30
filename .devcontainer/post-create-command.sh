#!/bin/sh
# This script is be used for the `postCreateCommand`
# There are 5 tasks involved in this script:
# 1. git config
# 2. setup lemonade client config for copying from remote container to local computer
# 3. git clone nevim configs
# 4. install python dependencies
# 5. setup terminal utils

# 1. git config
git config --global user.email "$GIT_EMAIL"
git config --global user.name "$GIT_NAME"
git config --global core.sshCommand 'ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no'

# 2. setup lemonade client for copying from remote container to local computer
if [ ! -d "$HOME"/.config ]; then
    mkdir "$HOME"/.config
fi
tee "$HOME"/.config/lemonade.toml <<EOF
port = 2489
host = '127.0.0.1'
line-ending = 'cr'
EOF

# 3. git clone nevim configs
git clone https://github.com/githubjacky/my-astronvim "$HOME"/.config/nvim

# 4. install python dependencies
if [ -f .devcontainer/requirements_devcontainer.txt ]; then
    uv pip install \
        --python /usr/local/python/current/bin/python \
        -r .devcontainer/requirements_devcontainer.txt
fi

# 5. setup terminal utils
wget https://gist.githubusercontent.com/githubjacky/20882d09ed0dd5d659a6b5a1336edbe8/raw/b264c149d328d76fb195b9497bfeee650edd4df2/setup.sh &&
    chmod +x setup.sh &&
    ./setup.sh &&
    rm -f setup.sh
