import admin from "./admin.js";
import common from "./common.js";
import fileView from "./fileView.js";
import markdown from "./markdown.js";
import mount from "./mount.js";
import upload from "./upload.js";
import fileBasket from "./fileBasket.js";
import search from "./search.js";
import pwa from "./pwa.js";
import gallery from "./gallery.js";

export default {
  ...admin,
  ...common,
  ...fileView,
  ...markdown,
  ...mount,
  ...upload,
  ...fileBasket,
  ...search,
  ...pwa,
  ...gallery,
  announcement: {
    title: "Announcement",
    dontShowAgain: "Don't show again",
    gotIt: "Got it",
  },
};
