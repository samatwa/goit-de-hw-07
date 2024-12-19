#!/bin/bash

# Перевірка параметрів
if [ $# -ne 2 ]; then
  echo "Використання: $0 <фінальний_архів> <приватний_ключ>"
  exit 1
fi

ENCRYPTED_ARCHIVE=$1
PRIVATE_KEY=$2

# Створюємо тимчасові директорії
TEMP_DIR="temp_parts"
DECRYPTED_DIR="decrypted_parts"
mkdir -p "$TEMP_DIR" "$DECRYPTED_DIR"

# Крок 1: Розпаковуємо зашифрований архів
echo "Розпаковуємо архів $ENCRYPTED_ARCHIVE..."
tar -xzf "$ENCRYPTED_ARCHIVE" -C "$TEMP_DIR"

# Крок 2: Дешифровуємо кожну частину
echo "Дешифруємо частини..."
for enc_part in "$TEMP_DIR"/part_*.enc; do
  OUTPUT_PART="${DECRYPTED_DIR}/$(basename "$enc_part" .enc)"
  openssl rsautl -decrypt -inkey "$PRIVATE_KEY" -in "$enc_part" -out "$OUTPUT_PART"
  echo "Дешифровано: $OUTPUT_PART"
done

# Крок 3: Об'єднуємо частини у вихідний архів
RECONSTRUCTED_ARCHIVE="reconstructed_archive.tar.gz"
echo "Об'єднуємо частини у архів $RECONSTRUCTED_ARCHIVE..."
cat "$DECRYPTED_DIR"/part_* > "$RECONSTRUCTED_ARCHIVE"

# Крок 4: Розпаковуємо архів
echo "Розпаковуємо архів для відновлення файлів..."
tar -xzf "$RECONSTRUCTED_ARCHIVE"

# Очищаємо тимчасові файли
rm -rf "$TEMP_DIR" "$DECRYPTED_DIR" "$RECONSTRUCTED_ARCHIVE"

echo "Розшифрування завершено. Файл успішно відновлено!"
