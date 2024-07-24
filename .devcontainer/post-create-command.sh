#!/bin/sh
# This script is used for the `postCreateCommand` in devcontainer.json
# There are 5 tasks involved in this script:
# 1. git config
# 2. install apt package
# 3. setup lemonade client for copying from remote container to local computer
# 4. clone nevim configs
# 5. install python dependencies
# 6. setup terminal utils

# 1. git config
git config --global user.email "$GIT_EMAIL"
git config --global user.name "$GIT_NAME"

# 2. install apt package
# python3.12-venv is required by mason.nivm to install python related lsp
# golang-go is for setting up the lemonade server
# npm is required to install language server
apt-get update &&
    apt-get install --no-install-recommends -y \
        build-essential python3.12-venv golang-go npm unzip

# 3. setup lemonade client for copying from remote container to local computer
if [ ! -d "$HOME"/.config ]; then
    mkdir "$HOME"/.config
fi
tee "$HOME"/.config/lemonade.toml << EOF
port = 2489
host = '127.0.0.1'
line-ending = 'cr'
EOF

go install github.com/lemonade-command/lemonade@latest
if [ ! -d "$HOME"/.local/bin ]; then
    mkdir -p "$HOME"/.local/bin
fi
ln -s /root/go/bin/lemonade "$HOME"/.local/bin

# 4. clone nevim configs
git clone https://github.com/githubjacky/my-astronvim "$HOME"/.config/nvim

# 5. install python dependencies
if [ -f .devcontainer/requirements.txt ]; then
    uv pip install \
    --python /usr/local/python/current/bin/python \
    -r .devcontainer/requirements.txt
fi

# 6. setup terminal utils
# replace with your own zsh config
wget https://gist.githubusercontent.com/githubjacky/20882d09ed0dd5d659a6b5a1336edbe8/raw/a20df5b3546ce7bca8db0a76d9b08d8d556f45b7/setup.sh
chmod +x setup.sh
./setup.sh
rm -f setup.sh
