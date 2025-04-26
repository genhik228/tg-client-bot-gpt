#!/usr/bin/env bash
set -e

main() {
  COMPOSE_FILE="docker-compose.yaml"
  ENV_FILE=".env"

  log() { echo "[$(date '+%F %T')] $1"; }

  # Проверка файлов
  [[ ! -f "$COMPOSE_FILE" ]] && log "Ошибка: $COMPOSE_FILE не найден!" && exit 1
  [[ ! -f "$ENV_FILE" ]] && log "Ошибка: $ENV_FILE не найден!" && exit 1

  # Запуск
  log "Запуск сервисов..."
  if ! docker-compose -f  up -d --build; then
    log "Ошибка при запуске контейнеров!"
    docker-compose logs --tail=50
    exit 1
  fi

  log "Проверка статуса:"
  docker-compose ps

  log "Сервисы запущены"
}

main "$@"