#!/usr/bin/env python3
"""Deployment script for Patient Matching API"""
import subprocess
import sys
import os

def run_command(cmd):
    """Run shell command and handle errors"""
    try:
        result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        print(f"✓ {cmd}")
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"✗ {cmd}")
        print(f"Error: {e.stderr}")
        sys.exit(1)

def deploy():
    """Deploy to AWS Lambda"""
    print("🚀 Deploying Patient Matching API...")
    
    # Install serverless if not present
    run_command("npm install -g serverless")
    run_command("npm install serverless-python-requirements")
    
    # Deploy
    run_command("serverless deploy")
    
    print("✅ Deployment complete!")

if __name__ == "__main__":
    deploy()