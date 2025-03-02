var result = db.tiktok_user_info.aggregate([
    {
        $facet: {
            "verified": [
                { $match: { is_verified: true } },
                { 
                    $project: {
                        username: 1,
                        _id: 0
                    }
                },
                { $sort: { username: 1 } }
            ],
            "not_verified": [
                { $match: { is_verified: false } },
                { 
                    $project: {
                        username: 1,
                        _id: 0
                    }
                },
                { $sort: { username: 1 } }
            ],
            "total": [
                { $count: "count" }
            ]
        }
    },
    {
        $project: {
            "Verified Users": "$verified.username",
            "Non-Verified Users": "$not_verified.username",
            "Verified Count": { $size: "$verified" },
            "Non-Verified Count": { $size: "$not_verified" },
            "Verified Percentage": {
                $multiply: [
                    { 
                        $divide: [
                            { $size: "$verified" },
                            { $arrayElemAt: ["$total.count", 0] }
                        ]
                    },
                    100
                ]
            },
            "Non-Verified Percentage": {
                $multiply: [
                    { 
                        $divide: [
                            { $size: "$not_verified" },
                            { $arrayElemAt: ["$total.count", 0] }
                        ]
                    },
                    100
                ]
            }
        }
    }
])

print(result)