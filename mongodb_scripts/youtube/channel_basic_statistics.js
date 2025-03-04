db.youtube_channel_stats.aggregate([
    {
      $project: {
        _id: 0,
        "Channel": "$title",
        "Views": { $toDouble: "$view_count" },
        "Subscribers": { $toDouble: "$subscriber_count" },
        "Videos": { $toDouble: "$video_count" },
        "Views Per Video": { 
          $divide: [
            { $toDouble: "$view_count" },
            { $cond: [
              { $eq: [{ $toDouble: "$video_count" }, 0] },
              1,
              { $toDouble: "$video_count" }
            ]}
          ]
        },
        "Views Per Subscriber": { 
          $divide: [
            { $toDouble: "$view_count" },
            { $cond: [
              { $eq: [{ $toDouble: "$subscriber_count" }, 0] },
              1,
              { $toDouble: "$subscriber_count" }
            ]}
          ]
        }
      }
    },
    {
      $sort: { "Subscribers": -1 }
    }
  ]).forEach(printjson)