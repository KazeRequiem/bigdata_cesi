# Journal d'audit déclaratif : chaque requête (lecture ou écriture de secret) est tracée sur la sortie standard.
# Les valeurs des secrets y sont hachées (HMAC), jamais en clair.
audit "file" "stdout" {
  description = "Journal d'audit sur la sortie standard du conteneur"
  options {
    file_path = "stdout"
  }
}
