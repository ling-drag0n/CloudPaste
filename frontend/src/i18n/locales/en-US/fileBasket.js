export default {
  fileBasket: {
    // Button text
    button: {
      empty: "File Basket",
      withCount: "File Basket ({count})",
    },

    // Panel titles and labels
    panel: {
      title: "File Basket",
      summary: "{fileCount} files from {directoryCount} directories",
      totalSize: "Total size: {size}MB",
      empty: "File basket is empty",
      emptyDescription: "Select files in the file browser and add them to the basket",
    },

    // Action buttons
    actions: {
      packDownload: "Pack & Download",
      clear: "Clear Basket",
      close: "Close",
      remove: "Remove",
      addToBasket: "Add to Basket",
      removeFromBasket: "Remove from Basket",
      batchAdd: "Batch Add",
      // Mobile short text
      mobile: {
        batchAdd: "Add",
      },
    },

    // Message prompts
    messages: {
      addSuccess: "Added {count} files to basket, total {total} files",
      addFailed: "Failed to add files to basket",
      removeSuccess: "File removed from basket",
      removeFailed: "Failed to remove file from basket",
      toggleFailed: "Failed to toggle basket status",
      batchAddSuccess: "Batch added {count} files to basket, total {total} files",
      batchAddFailed: "Failed to batch add files to basket",
      clearSuccess: "File basket cleared",
      clearFailed: "Failed to clear file basket",
      noFilesToAdd: "No files to add (folders will be ignored)",
      emptyBasket: "File basket is empty, please add files first",
      taskCreated: "Task created: {taskName}",
      taskCreateFailed: "Failed to create pack task",
      cancelled: "Operation cancelled",
    },

    // Warning messages
    warnings: {
      large: "Selected files total approximately {size}MB, packing may take some time",
      veryLarge: "Selected files total approximately {size}MB, packing may take a long time and consume significant memory",
    },

    // Confirmation dialogs
    confirmations: {
      continueAnyway: "Continue anyway?",
      clearBasket: "Are you sure you want to clear the file basket? This will remove all collected files.",
    },

    // Task related
    task: {
      name: "Pack download {count} files (from {directories} directories)",
      preparing: "Preparing download...",
      downloading: "Downloading files...",
      generating: "Generating archive...",
      completed: "Pack download completed",
      failed: "Pack download failed",
      summarySuccess: "Successfully downloaded {count} files",
      summaryWithFailures: "Success: {success}, Failed: {failed}",
      failedFilesHeader: "The following files failed to download:",
    },

    // Error messages
    errors: {
      noDownloadUrl: "Unable to get file download URL",
      downloadFailed: "File download failed",
      zipGenerationFailed: "Archive generation failed",
    },

    // File information
    fileInfo: {
      fileName: "File Name",
      fileSize: "Size",
      sourceDirectory: "Source Directory",
      addedTime: "Added Time",
    },

    // Status text
    status: {
      inBasket: "In basket",
      notInBasket: "Not in basket",
      processing: "Processing...",
      completed: "Completed",
      failed: "Failed",
    },
  },
};
