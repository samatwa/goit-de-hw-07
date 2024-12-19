# airflow_sandbox
Для шифрування файлів використовуємо скрипт `encrypt_and_archive.sh` та ключ `public_key.pem`. 
Приклад команди для шифрування: `./encrypt_and_archive.sh file_name public_key.pem`.
Скрипт сворить зашифрований архів `file_name_tar.gz`
Зашифровані файли складаємо в папку `encrypted_file`.
