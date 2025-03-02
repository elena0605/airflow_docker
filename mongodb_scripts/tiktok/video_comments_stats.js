db.tiktok_video_comments.aggregate([
    // First group by username and video_id
    {
      $group: {
        _id: {
          username: "$username",
          video_id: "$video_id"
        },
        comments_count: { $sum: 1 },
        total_likes: { $sum: "$like_count" },
        total_replies: { $sum: "$reply_count" },
        min_likes: { $min: "$like_count" },
        max_likes: { $max: "$like_count" },
        min_replies: { $min: "$reply_count" },
        max_replies: { $max: "$reply_count" },
        avg_likes: { $avg: "$like_count" },
        avg_replies: { $avg: "$reply_count" }
      }
    },
    
    // Then group by username to get all videos and overall stats
    {
      $group: {
        _id: "$_id.username",
        total_comments: { $sum: "$comments_count" },
        total_videos_commented: { $sum: 1 },
        total_likes_received: { $sum: "$total_likes" },
        total_replies_received: { $sum: "$total_replies" },
        
        // Details for each video in an array
        videos_with_comments: {
          $push: {
            video_id: "$_id.video_id",
            comments_count: "$comments_count",
            likes: "$total_likes",
            replies: "$total_replies",
            avg_likes_per_comment: { $round: ["$avg_likes", 2] },
            avg_replies_per_comment: { $round: ["$avg_replies", 2] },
            min_likes: "$min_likes",
            max_likes: "$max_likes", 
            min_replies: "$min_replies",
            max_replies: "$max_replies"
          }
        },
        
        // Min/max across all videos
        min_likes_on_any_video: { $min: "$min_likes" },
        max_likes_on_any_video: { $max: "$max_likes" },
        min_replies_on_any_video: { $min: "$min_replies" },
        max_replies_on_any_video: { $max: "$max_replies" },
        
        // Average engagement per comment across all videos
        avg_likes_per_comment_overall: { $avg: "$avg_likes" },
        avg_replies_per_comment_overall: { $avg: "$avg_replies" }
      }
    },
    
    // Sort videos within each username
    {
      $addFields: {
        videos_with_comments: {
          $sortArray: {
            input: "$videos_with_comments",
            sortBy: { comments_count: -1 }
          }
        },
        avg_comments_per_video: { 
          $round: [{ $divide: ["$total_comments", "$total_videos_commented"] }, 2]
        },
        avg_likes_per_video: {
          $round: [{ $divide: ["$total_likes_received", "$total_videos_commented"] }, 2]
        },
        avg_replies_per_video: {
          $round: [{ $divide: ["$total_replies_received", "$total_videos_commented"] }, 2]
        }
      }
    },
    
    // Format the output
    {
      $project: {
        _id: 0,
        username: "$_id",
        stats: {
          total_comments: "$total_comments",
          total_videos_with_comments: "$total_videos_commented",
          total_likes: "$total_likes_received",
          total_replies: "$total_replies_received",
          avg_comments_per_video: "$avg_comments_per_video",
          avg_likes_per_video: "$avg_likes_per_video",
          avg_replies_per_video: "$avg_replies_per_video",
          avg_likes_per_comment: { $round: ["$avg_likes_per_comment_overall", 2] },
          avg_replies_per_comment: { $round: ["$avg_replies_per_comment_overall", 2] },
          min_likes_across_videos: "$min_likes_on_any_video",
          max_likes_across_videos: "$max_likes_on_any_video",
          min_replies_across_videos: "$min_replies_on_any_video",
          max_replies_across_videos: "$max_replies_on_any_video"
        },
        videos_with_comments: 1
      }
    },
    
    // Sort by total comments (highest first)
    {
      $sort: { "stats.total_comments": -1 }
    }
  ]).forEach(printjson)