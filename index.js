require('/opt/node_modules/isomorphic-fetch');
const Client4 = require('/opt/node_modules/mattermost-redux/client/client4.js').default;
const client = new Client4();
const mattermostShared = require('/opt/mattermost-shared');

const defaultPageSize = process.env.DEFAULT_PAGE_SIZE;

const namesArray = [
    'posts',
    'chatReplies',
    'chatReactions',
    'chatAttachments',
    'postAttachmentRelationships',
    'postReactionRelationships',
    'channelViewers',
    'channelContributors',
    'channelJoinEvents',
    'channelLeaveEvents',
    'containsEdges',
];

exports.handler = async(event, context, callback) => {
    const validationError = mattermostShared.validateJobParams(event);
    if (validationError) {
        return callback(validationError);
    }

    const eventContext = event.context;
    const pagingContext = eventContext.execution.pagingContext;
    const pageDownloaderBucket = eventContext.config.pageDownloaderBucket;
    const startTime = eventContext.config.startTime;
    const maxChannelsToProcessPerLambdaCall = pagingContext.maxChannelsPerLambdaCall;
    const channelsPerPage = event.page.pageSize;
    const chunkStart = event.page.chunkStart || 0;
    const itemsToProcess = Math.min(maxChannelsToProcessPerLambdaCall, channelsPerPage - chunkStart);
    const chunkEnd = chunkStart + itemsToProcess;
    const s3Bucket = eventContext.config.pageDownloaderBucket;

    console.log('itemsToProcess: ', itemsToProcess);
    console.log('channelsPerPage: ', channelsPerPage);
    console.log('chunk start: ', chunkStart);
    console.log(`Working for page ${event.page.pageNumber} for chunk ${chunkStart}-${chunkEnd}`);

    client.setUrl(event.context.job.params.remoteSource);
    client.setToken(event.context.mattermost.token);

    try {
        const filenames = getFileNames(pagingContext);
        const fetchedObjects = await initializeFetchedObjects(chunkStart, filenames, pageDownloaderBucket);
        const channels = await mattermostShared.getFromDynamoDb('pg-mattermost-data', 'channel');

        console.log(`Got ${channels.length} channels from dynamoDb`);

        let promises = [];

        for (let i = chunkStart; i < chunkEnd; i++) {
            console.log(`Channel #${i} id: `, channels[i].id);
            promises.push(processChannelPosts(channels[i], fetchedObjects, defaultPageSize, startTime));
        }

        await Promise.all(promises);

        filterRemovedUsersFromEdges(fetchedObjects);
        filterMissingPostsFromEdges(fetchedObjects);

        await saveChatRepliesToDynamo(fetchedObjects);
        await uploadAllToS3(fetchedObjects, filenames, pageDownloaderBucket);

        event.page.chunkStart = chunkEnd;
        event.page.nextPageUrl = chunkEnd < channelsPerPage ? 'true' : '';

        eventContext.execution.dataLocation = {
            s3path: `s3://${s3Bucket}/${pagingContext.fileKey}`
        };

        return callback(null, event);
    } catch (err) {
        console.log(err);
        return callback(err);
    }
};

function filterRemovedUsersFromEdges(fetchedObjects) {
    const edgesArray = [
        'channelViewers',
        'channelContributors',
        'channelJoinEvents',
        'channelLeaveEvents',
    ];

    edgesArray.forEach(edge => {
        console.log(`${edge} length before: `, fetchedObjects[edge].length);
        fetchedObjects[edge] = fetchedObjects[edge].filter(edge => fetchedObjects.usernameByUserid[edge.from]);
        fetchedObjects[edge].forEach(edge => edge.id = `${edge.from}-viewerOf${edge.to}`);
        console.log(`${edge} length after: `, fetchedObjects[edge].length);
    });
}

async function saveChatRepliesToDynamo(fetchedObjects) {
    try {
        await mattermostShared.saveToDynamoDb('pg-mattermost-data', 'chatReply', fetchedObjects.chatReplies);
    }
    catch (error) {
        console.log('Could not save all chat replies to DynamoDB:', error);
    }
}

function filterMissingPostsFromEdges(fetchedObjects) {
    const postsTable = {};
    fetchedObjects.posts.forEach(post => postsTable[post.id] = {});
    fetchedObjects.chatReplies.forEach(post => postsTable[post.id] = {});

    console.log('postAttachmentRelationships length before: ', fetchedObjects.postAttachmentRelationships.length);
    fetchedObjects.postAttachmentRelationships = fetchedObjects.postAttachmentRelationships.filter(edge => postsTable[edge.to]);
    console.log('postAttachmentRelationships length after: ', fetchedObjects.postAttachmentRelationships.length);

    console.log('postReactionRelationships length before: ', fetchedObjects.postReactionRelationships.length);
    fetchedObjects.postReactionRelationships = fetchedObjects.postReactionRelationships.filter(edge => postsTable[edge.to]);
    console.log('postReactionRelationships length after: ', fetchedObjects.postReactionRelationships.length);
}

async function processChannelPosts(channel, fetchedObjects, pageSize, startTime) {
    const channelData = await intitializeChannelData(channel, pageSize, startTime);
    const posts = channelData.postsData.posts;
    const postIds = channelData.postsData.order;

    let promises = [];

    for (let i = 0; i < postIds.length; i++) {
        const key = postIds[i];
        const item = posts[key];
        if (item.create_at < startTime && item.update_at < startTime) {
            continue;
        }

        promises.push(readPostData(item, channelData, fetchedObjects));
    }

    await Promise.all(promises);

    saveViewersAndContributors(channelData, fetchedObjects);
}

function getFileNames(pagingContext) {
    const fileNames = {};

    fileNames.postsFileName = pagingContext.fileKey;
    fileNames.chatRepliesFileName = pagingContext.fileKey.replace('post.json', 'chat-reply.json');
    fileNames.chatAttachmentsFileName = pagingContext.fileKey.replace('post.json', 'chat-attachment.json');
    fileNames.postAttachmentRelationshipsFileName = pagingContext.fileKey.replace('post.json', 'post-to-attachment.json');
    fileNames.postReactionRelationshipsFileName = pagingContext.fileKey.replace('post.json', 'post-to-reaction.json');
    fileNames.chatReactionsFileName = pagingContext.fileKey.replace('post.json', 'chat-reaction.json');
    fileNames.channelJoinEventsFileName = pagingContext.fileKey.replace('post.json', 'channel-join.json');
    fileNames.channelLeaveEventsFileName = pagingContext.fileKey.replace('post.json', 'channel-leave.json');
    fileNames.channelContributorsFileName = pagingContext.fileKey.replace('post.json', 'channel-contributor.json');
    fileNames.channelViewersFileName = pagingContext.fileKey.replace('post.json', 'channel-viewer.json');
    fileNames.containsEdgesFileName = pagingContext.fileKey.replace('post.json', 'channel-contains-post.json');

    return fileNames;
}

async function initializeFetchedObjects(chunkStart, filenames, pageDownloaderBucket) {
    const fetchedObjects = {};
    namesArray.forEach(name => fetchedObjects[name] = []);
    await initializeUseridByUsername(fetchedObjects);

    return fetchedObjects;
}

function prepareBuffer(fetchedObjects, name) {
    fetchedObjects[`${name}Buffer`] = JSON.stringify(mattermostShared.convertAllFieldsToString(fetchedObjects[name]));
}

async function fillWithPreviousStep(fetchedObjects, filenames, name, pageDownloaderBucket) {
    console.log('key: ', name);
    let previousData = '';

    try {
        previousData = (await mattermostShared.getJsonFromS3(filenames[`${name}FileName`], pageDownloaderBucket)).Body.toString();
    }
    catch (error) {
        console.log('No previous data was found for: ', filenames[name + 'FileName'], pageDownloaderBucket, error);
    }

    if (previousData !== '[]') {
        const length = previousData.length;
        fetchedObjects[`${name}Buffer`] =
            previousData.substring(0, length - 1) + ',' + fetchedObjects[`${name}Buffer`].substr(1);
    }
}

async function readPostData(item, channelData, fetchedObjects) {
    const post = {
        id: item.id,
        sender: item.user_id,
        senddate: new Date(item.create_at).toISOString(),
        subject: channelData.channel.initialSubject,
        updatedate: new Date(item.update_at).toISOString(),
        channelid: item.channel_id,
        posttype: item.type,
        content: item.message,
        properties: JSON.stringify(item.props || item.properties),
        recipients: JSON.stringify(getRecipients(channelData, fetchedObjects)),
        parentid: item.parent_id,
        rootid: item.root_id,
    };

    await parsePostByType(post, channelData, fetchedObjects);
    parseMetadata(item, fetchedObjects, post, channelData);
}

function getRecipients(channelData, fetchedObjects) {
    return Object.keys(channelData.activeChannelMembers).filter(item => item);
}

async function getReadMessagesCountForMembers(channelId) {
    const members = 
        await mattermostShared.getEntitiesPage(defaultPageSize,
                                               page => client.getChannelMembers(channelId, page, defaultPageSize));
    const memberStats = {};
    if (Array.isArray(members)) {
        members.forEach(member => memberStats[member.user_id] = member.msg_count);
    }
    
    return memberStats;
}

function setMaxReadCountForMissingUsers(viewers, channelData) {
    viewers.forEach(viewer => {
        if (viewer.messagereadcount === 0) {
            viewer.messagereadcount = channelData.channel.messagecount;
        }
    });
}

async function saveViewersAndContributors(channelData, fetchedObjects) {
    console.log('Filling viewers and contributors for channel: ', channelData.channel.id);
    console.log('channelData.channelMembers: ', channelData.channelMembers);

    const memberStats = await getReadMessagesCountForMembers(channelData.channel.id);
    const channelMembersArray = Object.keys(channelData.channelMembers).map(userId => channelData.channelMembers[userId]);
    channelMembersArray.forEach(user => {
        if (user.lastMessageReadDate) {
            user.lastMessageReadDate = new Date(user.lastMessageReadDate).toISOString();
        }
        if (user.firstMessageReadDate) {
            user.firstMessageReadDate = new Date(user.firstMessageReadDate).toISOString();
        }
        if (user.lastMessageWriteDate) {
            user.lastMessageWriteDate = new Date(user.lastMessageWriteDate).toISOString();
        }
        if (user.firstMessageReadDate) {
            user.firstMessageReadDate = new Date(user.firstMessageReadDate).toISOString();
        }
    });

    const viewers = JSON.parse(JSON.stringify(channelMembersArray));
    setMaxReadCountForMissingUsers(viewers, channelData);
    viewers.forEach(viewer => {
        viewer.firstmessagedate = viewer.firstMessageReadDate || 0;
        viewer.lastmessagedate = viewer.lastMessageReadDate || 0;
        viewer.messagecount = channelData.channel.messagecount;
        if (memberStats[viewer.userId]) {
            viewer.messagereadcount = memberStats[viewer.userId];
        }
    });
    fetchedObjects.channelViewers = fetchedObjects.channelViewers.concat(viewers);

    const contributors = channelMembersArray.filter(user => user.messagecount > 0).slice(0);
    contributors.forEach(contributor => {
        contributor.firstmessagedate = contributor.firstMessageWriteDate || 0;
        contributor.lastmessagedate = contributor.lastMessageWriteDate || 0;
    });
    contributors.forEach(user => delete user.messagereadcount);
    fetchedObjects.channelContributors = fetchedObjects.channelContributors.concat(contributors);
}

async function uploadAllToS3(fetchedObjects, fileNames, pageDownloaderBucket) {
    for (let i = 0; i < namesArray.length; i++) {
        const name = namesArray[i];

        prepareBuffer(fetchedObjects, name);

        console.log(`${name} length before fillWithPreviousStep`, fetchedObjects[`${name}Buffer`].length);
        await fillWithPreviousStep(fetchedObjects, fileNames, name, pageDownloaderBucket);
        console.log(`${name} length after fillWithPreviousStep`, fetchedObjects[`${name}Buffer`].length);

        await mattermostShared.uploadJsonToS3(fetchedObjects[`${name}Buffer`], fileNames[name + 'FileName'], pageDownloaderBucket);
        console.log(`Saved ${name} to ${fileNames[name + 'FileName']}`, fetchedObjects[name]);

        delete fetchedObjects[`${name}Buffer`];
    }

}

async function fetchUserId(username) {
    try {
        const userFetched = await client.getUserByUsername(username);
        return userFetched.id;
    }
    catch (error) {
        console.log(`Error in fetchUserId(${username}):`, error);
        return false;
    }
}

function parseMetadata(item, fetchedObjects, post, channelData) {
    if (item.metadata && item.metadata.files) {
        item.metadata.files.forEach(file => {
            const attachmentNode = {
                id: file.id,
                filename: file.name,
                sender: post.sender,
                recipients: post.recipients,
                senddate: post.senddate,
                subject: post.subject,
                updatedate: post.updatedate,
            };
            fetchedObjects.chatAttachments.push(attachmentNode);
            fetchedObjects.postAttachmentRelationships.push({
                id: `${file.id}-${item.id}`,
                from: attachmentNode.id,
                to: item.id,
            });
            createContainsEdge(item.channel_id, attachmentNode.id, fetchedObjects);
        });
    }

    if (item.metadata && item.metadata.reactions) {
        let i = 0;
        item.metadata.reactions.forEach(reaction => {
            const reactionNode = {
                id: `${item.id}-reaction-${i}`,
                emojiname: reaction.emoji_name,
                sender: reaction.user_id,
                recipients: post.recipients,
                senddate: post.senddate,
                subject: post.subject,
                updatedate: post.updatedate,
            };
            fetchedObjects.chatReactions.push(reactionNode);
            fetchedObjects.postReactionRelationships.push({
                id: `${item.id}-reaction-${i}-relationship`,
                from: reactionNode.id,
                to: item.id,
            });
            increaseWriteCountForAuthor(channelData, reactionNode);
            createContainsEdge(item.channel_id, reactionNode.id, fetchedObjects);
            i++;
        });
    }
}

async function initializeUseridByUsername(fetchedObjects) {
    const usersData = await mattermostShared.getFromDynamoDb('pg-mattermost-data', 'user');
    const users = usersData.map(user => JSON.parse(user.json));
    console.log('Got users from dynamo db:', users);
    fetchedObjects.useridByUsername = {};
    fetchedObjects.usernameByUserid = {};
    users.forEach(user => fetchedObjects.useridByUsername[user.username] = user.id);
    users.forEach(user => fetchedObjects.usernameByUserid[user.id] = user.username);
    console.log('fetchedObjects.usernameByUserid:', fetchedObjects.usernameByUserid);
}

async function intitializeChannelData(channel, pageSize, startTime) {
    const downloadedChannelMembers = await mattermostShared.getFromDynamoDb('pg-mattermost-data', channel.id);
    console.log(`Got ${downloadedChannelMembers.length} channel members from dynamoDb for channel ${channel.id}`);

    const channelData = {};
    channelData.channel = JSON.parse(channel.json);
    channelData.channelMembers = {};
    channelData.activeChannelMembers = {};
    downloadedChannelMembers.forEach(member => {
        const user = {
            userId: member.user_id,
        };
        channelData.channelMembers[user.userId] = {
            id: `${user.userId}-memberOf${channel.id}`,
            from: user.userId,
            to: channel.id,
            messagecount: 0,
            messagereadcount: 0,
        };
        channelData.activeChannelMembers[user.userId] = {};
    });

    channelData.postsData = await getPosts(channel.id, pageSize, startTime);
    console.log('Got posts for channel', channel.id, ': ', channelData.postsData.order.length);

    return channelData;
}

function increaseReadCountForActiveChannelMembers(channelData, post) {
    Object.keys(channelData.activeChannelMembers).forEach(memberId => {
        const member = channelData.channelMembers[memberId];
        member.messagereadcount++;

        if (!member.firstMessageReadDate || member.firstMessageReadDate > post.senddate) {
            member.firstMessageReadDate = post.senddate;
        }
        if (!member.lastMessageReadDate || member.lastMessageReadDate < post.senddate) {
            member.lastMessageReadDate = post.senddate;
        }
    });
}

function increaseWriteCountForAuthor(channelData, post) {
    if (!channelData.channelMembers[post.sender]) {
        console.log('Adding an unexpected user! post id: ', post.id, ' channel id: ', channelData.channel.id, ' user id: ', post.sender);
        createNewChannelMember(channelData, post.sender);
    }

    const sender = channelData.channelMembers[post.sender];
    sender.messagecount++;

    if (!sender.firstMessageWriteDate || sender.firstMessageWriteDate > post.senddate) {
        sender.firstMessageWriteDate = post.senddate;
    }
    if (!sender.lastMessageWriteDate || sender.lastMessageWriteDate < post.senddate) {
        sender.lastMessageWriteDate = post.senddate;
    }
}

async function parsePostByType(post, channelData, fetchedObjects) {
    switch (post.posttype) {
        case 'system_join_channel':
        case 'system_add_to_team':
        case 'system_add_to_channel':
            console.log(`Post ${post.id} is add`);
            await addJoinEvent(post, channelData, fetchedObjects);
            break;
        case 'system_leave_channel':
        case 'system_remove_from_team':
        case 'system_remove_from_channel':
            console.log(`Post ${post.id} is leave`);
            addLeaveEvent(post, channelData, fetchedObjects);
            break;
        default:
            if (post.parentid) {
                console.log(`Post ${post.id} is chatReply`);
                fetchedObjects.chatReplies.push(post);
            }
            else {
                console.log(`Post ${post.id} is chatPost`);
                fetchedObjects.posts.push(post);
            }

            createContainsEdge(channelData.channel.id, post.id, fetchedObjects);

            increaseReadCountForActiveChannelMembers(channelData, post);
            increaseWriteCountForAuthor(channelData, post);
            break;
    }
}

async function addJoinEvent(post, channelData, fetchedObjects) {
    let userId;
    switch (post.posttype) {
        case 'system_join_channel':
            userId = post.sender;
            break;
        case 'system_add_to_team':
            userId = post.properties.addedUserId;
            break;
        case 'system_add_to_channel':
            const properties = JSON.parse(post.properties);
            userId = fetchedObjects.useridByUsername[properties.addedUsername];

            if (!userId) {
                userId = await fetchUserId(properties.addedUsername);
                if (!userId) {
                    return;
                }
            }

            break;
        default:
            throw new Error(`Invalid post type ${post.posttype} in addJoinEvent`);
    }

    fetchedObjects.channelJoinEvents.push({
        id: post.id,
        to: post.channelid,
        from: userId,
        properties: JSON.stringify(post.properties),
        joineddate: new Date(post.senddate).toISOString(),
    });

    delete channelData.activeChannelMembers[userId];
}

function addLeaveEvent(post, channelData, fetchedObjects) {
    console.log('Adding channelLeaveEvents: ', post.id);
    let userId;
    switch (post.posttype) {
        case 'system_leave_channel':
            userId = post.sender;
            break;
        case 'system_remove_from_channel':
            userId = post.properties.removedUserId;
            break;
        case 'system_remove_from_team':
            userId = post.sender;
            break;
        default:
            throw new Error(`Invalid post type ${post.posttype} in addLeaveEvent`);
    }

    fetchedObjects.channelLeaveEvents.push({
        id: post.id,
        to: post.channelid,
        from: userId,
        properties: JSON.stringify(post.properties),
        leftdate: new Date(post.senddate).toISOString(),
    });

    console.log('fetchedObjects.channelLeaveEvents: ', fetchedObjects.channelLeaveEvents);
    channelData.activeChannelMembers[userId] = {};

    if (!channelData.channelMembers[userId]) {
        createNewChannelMember(channelData, userId, fetchedObjects);
    }
}

function createNewChannelMember(channelData, userId) {
    channelData.channelMembers[userId] = {
        id: `${userId}-memberOf${channelData.channel.id}`,
        from: userId,
        to: channelData.channel.id,
        messagecount: 0,
        messagereadcount: 0,
    };
}

function createContainsEdge(channelId, messageId, fetchedObjects) {
    fetchedObjects.containsEdges.push({
        id: `${channelId}-contains-${messageId}`,
        from: channelId,
        to: messageId,
    });
}

async function getPosts(channelId, pageSize, startTime) {
    const posts = {
        order: [],
        posts: {}
    };
    let postsFetchedInOneCall = [];
    let page = 0;

    do {
        try {
            let hasFetched = false;
            while (!hasFetched) {
                try {
                   postsFetchedInOneCall = await client.getPosts(channelId, page, pageSize);
                   hasFetched = true;
                } catch (error) {
                    console.log('Error while getting posts from Mattermost: ', error);
                }
            }

            page++;
            if (!postsFetchedInOneCall) {
                break;
            }

            posts.order = posts.order.concat(postsFetchedInOneCall.order);
            Object.keys(postsFetchedInOneCall.posts).forEach(key => posts.posts[key] = postsFetchedInOneCall.posts[key]);
        }
        catch (error) {
            console.log('Error in getting posts from mattermost', error);
            throw error;
        }
    } while (postsFetchedInOneCall.order.length > 0);

    return posts;
}
