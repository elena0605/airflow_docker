  db.tiktok_user_info.aggregate([
    {
      $project: {
        username: 1,
        follower_count: { $ifNull: ["$follower_count", 0] }, // Handle missing follower_count
        likes_count: { $ifNull: ["$likes_count", 0] },       // Handle missing likes_count
        video_count: { $ifNull: ["$video_count", 0] },       // Handle missing video_count
  
        // Engagement Rate = (average likes per video) / follower_count * 100
        engagement_rate: {
          $cond: {
            if: { $and: [{ $gt: ["$video_count", 0] }, { $gt: ["$follower_count", 0] }] },
            then: {
              $round: [
                {
                  $multiply: [
                    {
                      $divide: [
                        { $divide: ["$likes_count", "$video_count"] }, // avg likes per video
                        "$follower_count"
                      ]
                    },
                    100 // Convert to percentage
                  ]
                },
                2
              ]
            },
            else: 0
          }
        },
  
        // Popularity Score = weighted combination of normalized metrics
        popularity_score: {
          $round: [
            {
              $multiply: [
                {
                  $add: [
                    { $multiply: [{ $divide: ["$follower_count", 1000000] }, 0.6] }, // 60% weight to followers (in millions)
                    { $multiply: [{ $divide: ["$likes_count", 1000000] }, 0.4] }     // 40% weight to likes (in millions)
                  ]
                },
                100 // Scale to 0-100 range
              ]
            },
            2
          ]
        },
        _id: 0
      }
    },
    {
      $sort: { popularity_score: -1 }
    },
    
  ]).forEach(printjson)

