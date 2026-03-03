#!/usr/bin/env bash

# docker-purge-all.sh
# A script to completely wipe all Docker containers, images, volumes, and networks.

echo "====================================================================="
echo "⚠️  WARNING: DOCKER PURGE SCRIPT INITIATED"
echo "====================================================================="
echo "This will completely destroy ALL Docker containers, images, volumes,"
echo "networks, and build caches. This action CANNOT be undone."
echo ""

read -p "Are you absolutely sure you want to proceed? (y/N): " confirm

if [[ "$confirm" != [yY] && "$confirm" != [yY][eE][sS] ]]; then
    echo "Purge aborted. Your Docker data is safe."
    exit 1
fi

echo ""
echo "🛑 Stopping all running containers..."
CONTAINERS=$(docker ps -aq)
if [ -n "$CONTAINERS" ]; then
    docker stop $CONTAINERS
    echo "🗑️  Removing all containers..."
    docker rm -f $CONTAINERS
else
    echo "✅ No containers found."
fi

echo ""
echo "💿 Removing all images..."
IMAGES=$(docker images -aq)
if [ -n "$IMAGES" ]; then
    docker rmi -f $IMAGES
else
    echo "✅ No images found."
fi

echo ""
echo "💾 Removing all volumes..."
VOLUMES=$(docker volume ls -q)
if [ -n "$VOLUMES" ]; then
    docker volume rm $VOLUMES
else
    echo "✅ No volumes found."
fi

echo ""
echo "🌐 Removing custom networks and lingering artifacts..."
docker system prune -a --volumes -f

echo ""
echo "🧹 Clearing builder cache..."
docker builder prune -a -f

echo ""
echo "✨ PURGE COMPLETE! Your Docker environment is a completely clean slate."
