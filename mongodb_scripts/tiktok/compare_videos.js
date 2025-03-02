 db.tiktok_user_info.aggregate([
    {
        $lookup: {
            from: "tiktok_user_video",
            localField: "username",
            foreignField: "username",
            as: "videos"
        }
    },
    {
        $project: {
            username: 1,
            declared_video_count: "$video_count",
            actual_video_count: { $size: "$videos" },
            _id: 0
        }
    },
    {
        $sort: { actual_video_count: -1 }
    }
]).forEach(printjson)

