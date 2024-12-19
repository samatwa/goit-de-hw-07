#!/bin/bash

# Перевірка параметрів
if [ $# -ne 2 ]; then
  echo "Використання: $0 <файл_для_шифрування> <публічний_ключ>"
  exit 1
fi

INPUT_FILE=$1
PUBLIC_KEY=$2
ARCHIVE_NAME="encrypted_parts.tar.gz"
PART_SIZE=245

# Створюємо тимчасову папку
TEMP_DIR="temp_parts"
mkdir -p "$TEMP_DIR"

# Крок 1: Розбиваємо файл на частини
echo "Розбиваємо файл $INPUT_FILE на частини по $PART_SIZE байтів..."
split -b $PART_SIZE "$INPUT_FILE" "$TEMP_DIR/part_"

# Крок 2: Шифруємо кожну частину за допомогою RSA
echo "Шифруємо частини..."
for part in "$TEMP_DIR"/part_*; do
  openssl rsautl -encrypt -pubin -inkey "$PUBLIC_KEY" -in "$part" -out "${part}.enc"
  rm "$part" # Видаляємо незашифровану частину
done

# Крок 3: Архівуємо зашифровані частини
echo "Створюємо архів $ARCHIVE_NAME..."
tar -czf "$ARCHIVE_NAME" -C "$TEMP_DIR" .

# Очищаємо тимчасову папку
rm -rf "$TEMP_DIR"

echo "Шифрування завершено. Архів створено: $ARCHIVE_NAME"
