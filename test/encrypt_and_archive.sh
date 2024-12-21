#!/bin/bash

# Перевірка параметрів
if [ $# -ne 2 ]; then
  echo "Використання: $0 <файл_для_шифрування> <публічний_ключ>"
  exit 1
fi

INPUT_FILE=$1
PUBLIC_KEY=$2

# Витягуємо ім'я файлу без шляху
BASENAME=$(basename "$INPUT_FILE")
ARCHIVE_NAME="${BASENAME}.tar.gz"
PART_SIZE=245

# Створюємо тимчасову папку
TEMP_DIR="temp_parts"
mkdir -p "$TEMP_DIR"

# Крок 1: Архівування файлу з ім'ям архіву, що відповідає імені файлу
echo "Архівування файлу $INPUT_FILE у $ARCHIVE_NAME..."
tar -czf "$ARCHIVE_NAME" "$INPUT_FILE"

# Крок 2: Розбиваємо архів на частини по $PART_SIZE байтів
echo "Розбиваємо архів на частини по $PART_SIZE байтів..."
split -b $PART_SIZE "$ARCHIVE_NAME" "$TEMP_DIR/part_"

# Крок 3: Шифруємо кожну частину за допомогою RSA
echo "Шифруємо частини..."
for part in "$TEMP_DIR"/part_*; do
  openssl rsautl -encrypt -inkey "$PUBLIC_KEY" -pubin -in "$part" -out "${part}.enc"
  rm "$part" # Видаляємо незашифровану частину
done

# Крок 4: Створюємо фінальний архів із зашифрованими частинами
FINAL_ARCHIVE="${BASENAME}_encrypted.tar.gz"
echo "Створюємо архів $FINAL_ARCHIVE із зашифрованими частинами..."
tar -czf "$FINAL_ARCHIVE" -C "$TEMP_DIR" .

# Очищаємо тимчасову папку та початковий архів
rm -rf "$TEMP_DIR" "$ARCHIVE_NAME"

echo "Шифрування завершено. Архів створено: $FINAL_ARCHIVE"
