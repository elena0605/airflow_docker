db.tiktok_user_info.find(
    {},
    { username: 1, likes_count: 1, _id: 0 }
  )
  .sort({ likes_count: -1 }).forEach(printjson)