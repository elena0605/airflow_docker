db.tiktok_user_info.find(
    {},
    { username: 1, follower_count: 1, _id: 0 }
  )
  .sort({ follower_count: -1 }).forEach(printjson)

