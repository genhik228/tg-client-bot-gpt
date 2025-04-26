#!/usr/bin/env bash
set -e

main() {
  # --- Настройки ---
  COMPOSE_FILE="docker-compose.yaml"
  ENV_FILE=".env"

  log() { echo "[$(date '+%F %T')] $1"; }

  # --- Проверка предварительных условий ---
  if [ ! -f "$COMPOSE_FILE" ]; then
    log "Ошибка: файл $COMPOSE_FILE не найден!"
    exit 1
  fi

  if [ ! -f "$ENV_FILE" ]; then
    log "Ошибка: файл $ENV_FILE не найден!"
    exit 1
  fi

  # --- Запуск сервисов ---
  log "Запускаем сервисы..."
  docker-compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" up -d --build

  log "Проверяем статус контейнеров..."
  docker-compose -f "$COMPOSE_FILE" ps

  log "Сервисы успешно запущены"
}

main "$@"