export default {
  backup: {
    title: "Data Backup & Restore",
    subtitle: "Manage system data backup and restore operations",
    operationLogs: "Operation Logs",

    // Backup operations
    backupOperations: {
      title: "Backup Operations",
      backupType: "Backup Type:",
      fullBackup: {
        title: "Full Backup",
        description: "Backup all data and configurations (Recommended)",
      },
      moduleBackup: {
        title: "Module Selection Backup",
        description: "Select specific functional modules for backup",
      },
      selectModules: "Select modules to backup:",
      selectedModules: "Selected {count} modules",
      createBackup: "Create Backup",
      creating: "Creating...",
    },

    // Restore operations
    restoreOperations: {
      title: "Restore Operations",
      selectFile: "Select Backup File",
      restoreMode: "Restore Mode:",
      overwriteMode: "Overwrite",
      mergeMode: "Merge",
      executeRestore: "Execute Restore",
      restoring: "Restoring...",
    },

    // Module definitions
    modules: {
      text_management: {
        name: "Text Management",
        description: "Text sharing data and passwords",
      },
      file_management: {
        name: "File Management",
        description: "File sharing data and passwords",
      },
      mount_management: {
        name: "Mount Management",
        description: "Storage mount point configurations",
      },
      storage_config: {
        name: "S3 Config Management",
        description: "S3 storage configuration information",
      },
      key_management: {
        name: "Key Management",
        description: "API key management",
      },
      account_management: {
        name: "Account Management",
        description: "Administrator accounts and tokens",
      },
      system_settings: {
        name: "System Settings",
        description: "System global settings",
      },
    },

    // Logs and status messages
    logs: {
      startFullBackup: "Starting full backup creation...",
      startModuleBackup: "Starting module backup creation ({count} modules)...",
      startRestore: "Starting restore execution ({mode} mode)...",
      backupComplete: "Backup creation completed, exported {count} records",
      moduleBackupComplete: "Module backup creation completed, exported {count} records",
      downloadStarted: "Backup file download started",
      restoreComplete: "Data restore completed, added {added} records, ignored {ignored} duplicates",
      restoreCompleteOverwrite: "Data restore completed, restored {count} records",
      tableExported: "{table} table: exported {count} records",
      tableRestored: "{table} table: added {added}, ignored {ignored}, expected {expected} (partially existing)",
      tableRestoredOverwrite: "{table} table: success {success}, expected {expected}",
      gettingModuleInfo: "Getting module information...",
      moduleInfoSuccess: "Module information retrieved successfully",
      recordsCount: "{count} records",
      clearLogs: "Clear Logs",
    },

    // Error messages
    errors: {
      selectAtLeastOneModule: "Please select at least one module",
      getModuleInfoFailed: "Failed to get module information: {error}",
      backupFailed: "Backup creation failed: {error}",
      restoreFailed: "Data restore failed: {error}",
      fileSelectRequired: "Please select a backup file first",
      invalidBackupFile: "Invalid backup file format",
      parseBackupFailed: "Failed to parse backup file: {error}",
    },

    // Success messages
    success: {
      backupCreated: "Backup created successfully",
      dataRestored: "Data restored successfully",
    },

    // Confirmation dialogs
    confirmations: {
      restoreOverwrite: "Overwrite mode will replace existing data, this operation cannot be undone. Are you sure you want to continue?",
      restoreMerge: "Merge mode will keep existing data and add new data. Are you sure you want to continue?",
    },
  },
};
