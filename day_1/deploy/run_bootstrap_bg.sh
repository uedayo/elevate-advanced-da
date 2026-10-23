#!/usr/bin/env bash
export PATH="/usr/local/google/home/uedayo/.local/share/uv/python/cpython-3.12-linux-x86_64-gnu/bin:/usr/local/google/home/uedayo/google-cloud-sdk/bin:$HOME/.terraform/bin:$HOME/.local/bin:$PATH"
export CLOUDSDK_CONTEXT_AWARE_USE_CLIENT_CERTIFICATE=false
export GOOGLE_API_USE_CLIENT_CERTIFICATE=false
export GOOGLE_CLOUD_QUOTA_PROJECT=argolisyo
cd "$HOME/workspace/elevate-da-adv-learner/day_1/deploy"
exec "$HOME/.terraform/bin/terraform" apply -auto-approve > bootstrap_output.txt 2> bootstrap_errors.txt
