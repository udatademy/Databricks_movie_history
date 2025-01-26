# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount(Montar) Azure Data Lake para el Proyecto

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    # Obtener Secret Key de Key Vault
    client_id = dbutils.secrets.get(scope = "movie-history-secret-scope", key = "client-id")
    tenant_id = dbutils.secrets.get(scope = "movie-history-secret-scope", key = "tenant-id")
    client_secret = dbutils.secrets.get(scope = "movie-history-secret-scope", key = "client-secret")

    # Establecer configuraciones de Spark
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

    # Desmontar(unmount) el montaje(mount) si ya existe
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
          dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    # Mount(Montar) el Contenedor del Storage Account
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

    # Listar los Mount
    display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Montar(mount) el contenedor "bronze"

# COMMAND ----------

mount_adls("moviehistory", "bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Montar(mount) el contenedor "silver"

# COMMAND ----------

mount_adls("moviehistory", "silver")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Montar(mount) el contenedor "gold"

# COMMAND ----------

mount_adls("moviehistory", "gold")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Montar(mount) el contenedor "demo"

# COMMAND ----------

mount_adls("moviehistory", "demo")
