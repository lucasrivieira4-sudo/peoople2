const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mongoose = require('mongoose');
const axios = require('axios');
const cors = require('cors');
const compression = require('compression');
const path = require('path');
const FormData = require('form-data');
const XLSX = require('xlsx');
require('dotenv').config();

const app = express();
const server = http.createServer(app);

// ============================================
// SOCKET.IO
// ============================================
const io = new Server(server, {
    cors: { origin: "*", methods: ["GET", "POST"] },
    pingTimeout: 10000,
    pingInterval: 5000,
    transports: ['websocket', 'polling'],
    upgradeTimeout: 5000
});

// ============================================
// MIDDLEWARES
// ============================================
app.use(compression());
app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));
app.use(express.static(path.join(__dirname)));
// Também funciona com a estrutura deste repositório (backend/ + public/crm/).
app.use(express.static(path.join(__dirname, '../public/crm')));

let templateCache = { data: [], lastUpdate: 0 };

// ============================================
// CACHE DE CONFIG — evita query ao banco em cada mensagem
// ============================================
let _configCache = null;
let _configCacheTime = 0;
const CONFIG_CACHE_TTL = 60000; // 1 minuto

async function getConfig() {
    const now = Date.now();
    if (_configCache && (now - _configCacheTime) < CONFIG_CACHE_TTL) {
        return _configCache;
    }
    _configCache = await Config.findOne().lean();
    _configCacheTime = now;
    return _configCache;
}

// ============================================
// HEALTHCHECK
// ============================================
app.get('/health', (req, res) => {
    res.status(200).json({ 
        status: 'ok', 
        timestamp: new Date().toISOString(),
        mongoConnected: mongoose.connection.readyState === 1
    });
});

// ============================================
// FUNÇÃO: Normalização de Telefone
// ============================================
function normalizePhone(phone) {
    if (!phone) return phone;
    let clean = phone.replace(/\D/g, '');
    if (clean.startsWith('0')) clean = clean.slice(1);
    if (clean.length === 10 || clean.length === 11) clean = '55' + clean;
    if (clean.startsWith('55') && clean.length === 13 && clean[4] === '9') {
        return clean.slice(0, 4) + clean.slice(5);
    }
    return clean;
}

function getButtonResponse(msg) {
    if (msg?.type === 'button' && msg.button) {
        return {
            text: msg.button.text || msg.button.payload || '[BOTÃO]',
            payload: msg.button.payload || null
        };
    }

    if (msg?.type === 'interactive' && msg.interactive) {
        const buttonReply = msg.interactive.button_reply;
        const listReply = msg.interactive.list_reply;
        return {
            text: buttonReply?.title
                || listReply?.title
                || listReply?.description
                || buttonReply?.id
                || listReply?.id
                || '[INTERATIVO]',
            payload: buttonReply?.id || listReply?.id || null
        };
    }

    return null;
}

function buildLeadUpdate({ phone, name, tags, clearTags }) {
    const update = {
        $set: { name, updatedAt: new Date() },
        // A field must not appear in both $set and $setOnInsert.
        $setOnInsert: { phone }
    };

    if (clearTags === true) {
        update.$set.tags = [];
    } else if (Array.isArray(tags) && tags.length > 0) {
        update.$set.tags = [...new Set(tags.map(String))];
    } else {
        update.$setOnInsert.tags = [];
    }

    return update;
}

// ============================================
// MONGODB ATLAS - Conexão Otimizada
// ============================================

const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/peoople-crm';

function getMongoURI() {
    let uri = MONGODB_URI;
    const separator = uri.includes('?') ? '&' : '?';
    const options = [
        'retryWrites=true',
        'w=majority',
        'maxPoolSize=10',
        'serverSelectionTimeoutMS=30000',
        'socketTimeoutMS=45000',
        'connectTimeoutMS=30000',
        'heartbeatFrequencyMS=10000',
        'tls=true',
        'tlsAllowInvalidCertificates=false'
    ];
    for (const opt of options) {
        const key = opt.split('=')[0];
        if (!uri.includes(key + '=')) {
            uri += separator + opt;
        }
    }
    return uri;
}

console.log('🔌 Iniciando conexão MongoDB Atlas...');

const mongoURI = getMongoURI();
console.log('URI (oculta):', mongoURI.replace(/\/\/[^:]+:[^@]+@/, '//***:***@'));

const connectWithRetry = async (retries = 5, delay = 5000) => {
    for (let i = 0; i < retries; i++) {
        try {
            await mongoose.connect(mongoURI);
            console.log('✅ MongoDB Atlas Conectado!');
            return true;
        } catch (err) {
            console.error(`❌ Tentativa ${i + 1}/${retries}:`, err.message);
            if (i < retries - 1) {
                console.log(`⏳ Retry em ${delay}ms...`);
                await new Promise(r => setTimeout(r, delay));
            }
        }
    }
    console.error('❌ Falha permanente na conexão MongoDB');
    return false;
};

connectWithRetry().then(connected => {
    if (!connected) {
        console.log('⚠️  Servidor iniciando sem MongoDB (modo degradado)');
    }
});

mongoose.connection.on('connected', () => console.log('🟢 MongoDB: Connected'));
mongoose.connection.on('error', (err) => console.error('🔴 MongoDB Error:', err.message));
mongoose.connection.on('disconnected', () => console.warn('🟡 MongoDB: Disconnected'));

// ============================================
// SCHEMAS
// ============================================

const ConfigSchema = new mongoose.Schema({
    profileName: String,
    profilePicture: String,
    metaAccessToken: String,
    metaPhoneNumberId: String,
    metaBusinessAccountId: String,
    metaWebhookVerifyToken: String,
    tagColors: { type: Map, of: String, default: {} },
    // MongoDB aceita este campo opcional sem migração da base existente.
    workedTags: { type: [String], default: [] }
}, { minimize: false });

const LeadSchema = new mongoose.Schema({
    name: String,
    phone: { type: String, unique: true, index: true },
    tags: [String],
    lastInteraction: { type: Date, default: Date.now, index: true },
    updatedAt: { type: Date, default: Date.now, index: true }
}, { timestamps: true });

const MessageSchema = new mongoose.Schema({
    phone: { type: String, required: true, index: true },
    text: { type: String, default: '' },
    direction: { type: String, enum: ['sent', 'received'], required: true, index: true },
    timestamp: { type: Date, default: Date.now, required: true, index: true },
    type: { type: String, default: 'text' },
    buttonPayload: { type: String, default: null },
    reactionToWamid: { type: String, default: null },
    reactionEmoji: { type: String, default: null },
    mediaData: { type: String, default: null },
    mediaFileId: { type: mongoose.Schema.Types.ObjectId, default: null },
    mediaMimeType: { type: String, default: null },
    metaMediaId: { type: String, default: null },
    status: { type: String, default: 'sent', index: true },
    wamid: { type: String, index: true, sparse: true },
    fileName: { type: String, default: null },
    error: { type: String, default: null },
    locationData: {
        lat: { type: Number, default: null },
        lng: { type: Number, default: null },
        name: { type: String, default: null },
        address: { type: String, default: null }
    }
});

MessageSchema.index({ phone: 1, timestamp: -1 });
MessageSchema.index({ direction: 1, phone: 1, timestamp: -1 });

const QuickReplySchema = new mongoose.Schema({
    shortcut: String,
    text: String,
    mediaData: String,
    mediaName: String,
    mediaType: String
});

const RemarketingCampaignSchema = new mongoose.Schema({
    name: { type: String, required: true, trim: true },
    roundName: { type: String, required: true, trim: true },
    roundKey: { type: String, required: true, index: true },
    tags: { type: [String], default: [] },
    templateName: { type: String, required: true },
    languageCode: { type: String, default: 'pt_BR' },
    templateText: { type: String, default: '' },
    headerMediaId: { type: String, default: null },
    createdAt: { type: Date, default: Date.now },
    updatedAt: { type: Date, default: Date.now }
}, { minimize: false });

const RemarketingRecipientSchema = new mongoose.Schema({
    campaignId: { type: mongoose.Schema.Types.ObjectId, ref: 'RemarketingCampaign', required: true, index: true },
    roundKey: { type: String, required: true },
    phone: { type: String, required: true },
    name: { type: String, default: '' },
    tags: { type: [String], default: [] },
    status: {
        type: String,
        enum: ['pending', 'sending', 'sent', 'failed', 'uncertain'],
        default: 'pending',
        index: true
    },
    attempts: { type: Number, default: 0 },
    wamid: { type: String, default: null },
    error: { type: String, default: null },
    startedAt: { type: Date, default: null },
    sentAt: { type: Date, default: null },
    createdAt: { type: Date, default: Date.now }
}, { minimize: false });

// O mesmo telefone só pode ser atribuído uma vez a cada rodada, inclusive
// quando duas abas tentam criar campanhas ao mesmo tempo.
RemarketingRecipientSchema.index({ roundKey: 1, phone: 1 }, { unique: true });

const Config = mongoose.model('Config', ConfigSchema);
const Lead = mongoose.model('Lead', LeadSchema);
const Message = mongoose.model('Message', MessageSchema);
const QuickReply = mongoose.model('QuickReply', QuickReplySchema);
const RemarketingCampaign = mongoose.model('RemarketingCampaign', RemarketingCampaignSchema);
const RemarketingRecipient = mongoose.model('RemarketingRecipient', RemarketingRecipientSchema);

const MAX_META_VIDEO_BYTES = 16 * 1024 * 1024;
const VIDEO_BUCKET_NAME = 'messageVideos';
const META_GRAPH_API_VERSION = 'v26.0';
const REMARKETING_COST_PER_MESSAGE = 0.32;

function normalizeRemarketingRound(value) {
    return String(value || '')
        .trim()
        .toLowerCase()
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .replace(/(\d)[ªº]/g, '$1')
        .replace(/(\d)a(?=[\s-]|$)/g, '$1')
        .replace(/[^a-z0-9]+/g, '-')
        .replace(/^-|-$/g, '')
        .slice(0, 80);
}

// A aceitação da API e a entrega são estados diferentes. O histórico da mensagem
// é a fonte dos retornos assíncronos, inclusive para campanhas antigas.
function getCampaignDeliveryStages() {
    return [
        { $lookup: {
            from: Message.collection.name,
            let: { messageId: '$wamid' },
            pipeline: [
                { $match: { $expr: { $and: [
                    { $ne: [{ $ifNull: ['$$messageId', null] }, null] },
                    { $ne: ['$$messageId', ''] },
                    { $eq: ['$wamid', '$$messageId'] }, { $eq: ['$direction', 'sent'] }
                ] } } },
                // Se houver registros antigos duplicados, uma entrega comprovada prevalece.
                { $set: { deliveryRank: { $switch: { branches: [
                    { case: { $eq: ['$status', 'read'] }, then: 4 },
                    { case: { $eq: ['$status', 'delivered'] }, then: 3 },
                    { case: { $eq: ['$status', 'failed'] }, then: 2 }
                ], default: 1 } } } },
                { $sort: { deliveryRank: -1, timestamp: -1, _id: -1 } },
                { $project: { status: 1, error: 1 } }, { $limit: 1 }
            ], as: 'deliveryMessage'
        } },
        { $set: { deliveryMessage: { $arrayElemAt: ['$deliveryMessage', 0] } } },
        { $set: {
            deliveryStatus: { $switch: { branches: [
                { case: { $eq: ['$status', 'pending'] }, then: 'pending' },
                { case: { $in: ['$deliveryMessage.status', ['delivered', 'read']] }, then: '$deliveryMessage.status' },
                { case: { $or: [{ $eq: ['$status', 'failed'] }, { $eq: ['$deliveryMessage.status', 'failed'] }] }, then: 'failed' },
                { case: { $eq: ['$status', 'sent'] }, then: 'accepted' }
            ], default: 'uncertain' } },
            deliveryError: { $ifNull: ['$deliveryMessage.error', '$error'] }
        } }
    ];
}

async function getRemarketingCampaignSummaries(campaigns, { includeDeliveries = true } = {}) {
    if (!campaigns.length) return [];

    const campaignIds = campaigns.map(campaign => campaign._id);
    // A lista inicial precisa somente das reservas e tentativas. O cruzamento
    // com mensagens fica no relatório da campanha, sem atrasar a preparação.
    const groups = await RemarketingRecipient.aggregate([
        { $match: { campaignId: { $in: campaignIds } } },
        ...(includeDeliveries ? getCampaignDeliveryStages() : []),
        { $group: { _id: { campaignId: '$campaignId', status: '$status', deliveryStatus: '$deliveryStatus' }, count: { $sum: 1 } } }
    ]);
    const countsByCampaign = new Map();
    const deliveriesByCampaign = new Map();
    for (const group of groups) {
        const key = String(group._id.campaignId);
        const counts = countsByCampaign.get(key) || {};
        counts[group._id.status] = (counts[group._id.status] || 0) + group.count;
        countsByCampaign.set(key, counts);
        const deliveries = deliveriesByCampaign.get(key) || {};
        deliveries[group._id.deliveryStatus] = (deliveries[group._id.deliveryStatus] || 0) + group.count;
        deliveriesByCampaign.set(key, deliveries);
    }

    return campaigns.map(campaign => {
        const counts = countsByCampaign.get(String(campaign._id)) || {};
        const pendingCount = counts.pending || 0;
        const sendingCount = counts.sending || 0;
        const sentCount = counts.sent || 0;
        const failedCount = counts.failed || 0;
        const uncertainCount = counts.uncertain || 0;
        const deliveries = deliveriesByCampaign.get(String(campaign._id)) || {};
        const deliveryFailedCount = deliveries.failed || 0;
        const deliveredCount = (deliveries.delivered || 0) + (deliveries.read || 0);
        const readCount = deliveries.read || 0;
        const unconfirmedCount = (deliveries.accepted || 0) + (deliveries.uncertain || 0);
        const totalRecipients = pendingCount + sendingCount + sentCount + failedCount + uncertainCount;
        const state = pendingCount + sendingCount + failedCount + uncertainCount === 0
            ? 'completed'
            : (sentCount > 0 || sendingCount > 0 ? 'in_progress' : 'draft');

        return {
            ...campaign,
            totalRecipients,
            pendingCount,
            sendingCount,
            sentCount,
            failedCount,
            uncertainCount,
            deliveryFailedCount: includeDeliveries ? deliveryFailedCount : null,
            deliveredCount: includeDeliveries ? deliveredCount : null,
            readCount: includeDeliveries ? readCount : null,
            unconfirmedCount: includeDeliveries ? unconfirmedCount : null,
            deliverySummaryPending: !includeDeliveries,
            estimatedTotalCost: totalRecipients * REMARKETING_COST_PER_MESSAGE,
            estimatedSentCost: sentCount * REMARKETING_COST_PER_MESSAGE,
            state
        };
    });
}

async function setRemarketingRecipientStatus(recipientId, status, details = {}) {
    const update = { $set: { status, ...details } };
    if (status === 'pending') update.$unset = { startedAt: 1 };

    const result = await RemarketingRecipient.updateOne(
        { _id: recipientId, status: 'sending' },
        update
    );
    return result.modifiedCount === 1;
}

function getVideoBucket() {
    if (!mongoose.connection.db) throw new Error('MongoDB ainda não está conectado');
    if (!mongoose.mongo?.GridFSBucket) throw new Error('GridFS não está disponível nesta versão do MongoDB driver');
    return new mongoose.mongo.GridFSBucket(mongoose.connection.db, { bucketName: VIDEO_BUCKET_NAME });
}

function getSupportedVideoMimeType(mimeType, fileName) {
    const normalizedMime = String(mimeType || '').split(';')[0].trim().toLowerCase();
    const extension = String(fileName || '').toLowerCase().split('.').pop();
    if (normalizedMime === 'video/mp4' || extension === 'mp4') return 'video/mp4';
    if (normalizedMime === 'video/3gpp' || extension === '3gp') return 'video/3gpp';
    return null;
}

function getDataUrlBuffer(dataUrl) {
    const match = String(dataUrl || '').match(/^data:[^;]+;base64,(.+)$/);
    if (!match) throw new Error('Formato de mídia inválido');
    return Buffer.from(match[1], 'base64');
}

function saveVideoToGridFS(buffer, fileName, mimeType) {
    return new Promise((resolve, reject) => {
        let uploadStream;
        try {
            uploadStream = getVideoBucket().openUploadStream(fileName || 'video.mp4', {
                metadata: { contentType: mimeType || 'video/mp4' }
            });
        } catch (error) {
            reject(error);
            return;
        }
        uploadStream.once('error', reject);
        uploadStream.once('finish', () => resolve(uploadStream.id));
        uploadStream.end(buffer);
    });
}

function readVideoFromGridFS(fileId) {
    return new Promise((resolve, reject) => {
        let downloadStream;
        try {
            const id = fileId instanceof mongoose.mongo.ObjectId
                ? fileId
                : new mongoose.mongo.ObjectId(String(fileId));
            downloadStream = getVideoBucket().openDownloadStream(id);
        } catch (error) {
            reject(error);
            return;
        }
        const chunks = [];
        downloadStream.on('data', chunk => chunks.push(chunk));
        downloadStream.once('error', reject);
        downloadStream.once('end', () => resolve(Buffer.concat(chunks)));
    });
}

async function downloadMetaMedia(mediaId, config) {
    if (!mediaId) throw new Error('Vídeo não encontrado');
    const mediaRes = await axios.get(
        `https://graph.facebook.com/${META_GRAPH_API_VERSION}/${mediaId}`,
        { headers: { Authorization: `Bearer ${config.metaAccessToken}` }, timeout: 15000 }
    );
    const fileRes = await axios.get(mediaRes.data.url, {
        headers: { Authorization: `Bearer ${config.metaAccessToken}` },
        responseType: 'arraybuffer',
        timeout: 30000
    });
    return {
        buffer: Buffer.from(fileRes.data),
        mimeType: mediaRes.data.mime_type || fileRes.headers?.['content-type'] || null
    };
}

function streamGridFsFile(res, fileId, mimeType, fileName, disposition = 'inline') {
    const safeName = String(fileName || 'video.mp4').replace(/["\\\r\n]/g, '');
    res.setHeader('Content-Type', mimeType || 'video/mp4');
    res.setHeader('Content-Disposition', `${disposition}; filename="${safeName}"`);
    const stream = getVideoBucket().openDownloadStream(
        fileId instanceof mongoose.mongo.ObjectId ? fileId : new mongoose.mongo.ObjectId(String(fileId))
    );
    stream.once('error', error => {
        console.error('Erro ao ler vídeo no MongoDB:', error.message);
        if (!res.headersSent) res.status(404).json({ error: 'Vídeo não encontrado no armazenamento' });
        else res.destroy(error);
    });
    stream.pipe(res);
}

// ============================================
// SOCKET.IO
// ============================================
io.on('connection', (socket) => {
    console.log('Cliente:', socket.id);
    
    socket.on('joinChat', (phone) => {
        const normalized = normalizePhone(phone);
        socket.rooms.forEach(r => { if (r !== socket.id) socket.leave(r); });
        socket.join(normalized);
    });
    
    socket.on('disconnect', () => {});
});

// ============================================
// WEBHOOK META
// ============================================
app.get('/api/webhook', (req, res) => {
    const token =
        process.env.META_WEBHOOK_VERIFY_TOKEN ||
        'peoople_token';

    if (req.query['hub.verify_token'] === token) {
        return res.status(200).send(req.query['hub.challenge']);
    }

    return res.sendStatus(403);
});

app.post('/api/webhook', async (req, res) => {
    res.sendStatus(200);
    
    setImmediate(async () => {
        try {
            const entry = req.body.entry?.[0];
            const changes = entry?.changes?.[0]?.value;
            if (!changes) return;
            
            // Um callback pode conter várias entradas e alterações. Não ignora as demais.
            const statuses = (req.body.entry || []).flatMap(item =>
                (item.changes || []).flatMap(change => change.value?.statuses || []));
            // Atualização de status não depende de token de envio válido.
            for (const status of statuses) {
                if (!status.id || !['sent', 'delivered', 'read', 'failed'].includes(status.status)) continue;
                const errorText = (status.errors || []).map(error =>
                    `[${error.code}] ${error.title || error.message || ''}${error.error_data?.details ? ' — ' + error.error_data.details : ''}`
                ).join(' | ') || null;
                // O retorno pode chegar antes de o salvamento em background terminar.
                // Guarda por wamid mesmo nesse caso, sem depender de ter a conversa aberta.
                if (status.recipient_id) {
                    await Message.updateOne(
                        { wamid: status.id, direction: 'sent' },
                        { $setOnInsert: {
                            phone: normalizePhone(status.recipient_id), direction: 'sent',
                            text: '[Envio registrado pelo retorno da Meta]',
                            timestamp: new Date(), status: 'sent'
                        } },
                        { upsert: true }
                    );
                }
                // Webhooks podem chegar repetidos ou fora de ordem: não rebaixa uma entrega confirmada.
                const filter = { wamid: status.id };
                if (status.status === 'sent') filter.status = { $nin: ['delivered', 'read', 'failed'] };
                if (status.status === 'delivered' || status.status === 'failed') filter.status = { $ne: 'read' };
                if (status.status === 'failed') filter.status = { $nin: ['delivered', 'read'] };
                const msg = await Message.findOneAndUpdate(
                    filter,
                    { $set: { status: status.status, error: status.status === 'failed' ? errorText : null } },
                    { new: true, lean: true }
                );
                if (msg) io.to(msg.phone).emit('statusUpdate', {
                    wamid: status.id,
                    status: status.status,
                    error: errorText
                });
            }

            // Usa cache para o recebimento/download de mídia, não para os status.
            const config = await getConfig();
            if (!config?.metaAccessToken) return;

            // Nova mensagem recebida
            if (changes?.messages?.[0]) {
                const msg = changes.messages[0];
                const contact = changes.contacts?.[0];
                let phone = normalizePhone(msg.from);
                let text = msg.text?.body || "";
                let type = msg.type || 'text';
                let fileName = null;
                let locationData = null;
                let buttonPayload = null;
                let reactionToWamid = null;
                let reactionEmoji = null;
                const isMedia = ['image', 'audio', 'video', 'document'].includes(type);

                if (isMedia) {
                    fileName = msg[type]?.filename || null;
                    text = msg[type]?.caption || `[${type.toUpperCase()}]`;
                }

                if (type === 'reaction') {
                    reactionToWamid = msg.reaction?.message_id || null;
                    reactionEmoji = String(msg.reaction?.emoji || '').trim() || null;
                    text = reactionEmoji
                        ? `Reagiu com ${reactionEmoji}`
                        : 'Removeu a reação';
                }

                if (type === 'location' && msg.location) {
                    locationData = {
                        lat: msg.location.latitude,
                        lng: msg.location.longitude,
                        name: msg.location.name || null,
                        address: msg.location.address || null
                    };
                    const label = locationData.name || locationData.address || null;
                    text = label ? `📍 ${label}` : '📍 Localização compartilhada';
                }

                // Contato encaminhado (cartão de contato do WhatsApp)
                if (type === 'contacts' && Array.isArray(msg.contacts) && msg.contacts.length) {
                    text = msg.contacts.map(c => {
                        const name = c?.name?.formatted_name || 'Contato sem nome';
                        const number = c?.phones?.[0]?.phone || c?.phones?.[0]?.wa_id || '';
                        return `👤 ${name}${number ? ' - ' + number : ''}`;
                    }).join('\n');
                }

                // Sticker (figurinha)
                if (type === 'sticker') {
                    text = '🖼️ Figurinha';
                }

                // Normaliza respostas de botões rápidos, botões interativos e listas.
                const buttonResponse = getButtonResponse(msg);
                if (buttonResponse) {
                    text = buttonResponse.text;
                    buttonPayload = buttonResponse.payload;
                }

                // Fallback: nunca deixa a mensagem sem texto (evita sumir da lista de conversas)
                if (!text) {
                    text = `[${String(type).toUpperCase()}]`;
                }

                // SALVA E EMITE IMEDIATAMENTE — sem esperar o download da mídia
                const savedMsg = await Message.create({
                    phone, text, direction: 'received', type,
                    buttonPayload,
                    reactionToWamid,
                    reactionEmoji,
                    mediaData: null, wamid: msg.id, fileName,
                    mediaMimeType: msg[type]?.mime_type || null,
                    metaMediaId: isMedia ? (msg[type]?.id || null) : null,
                    locationData: locationData || undefined
                });

                let lead = await Lead.findOneAndUpdate(
                    { phone },
                    { updatedAt: new Date() },
                    { new: true, upsert: true, lean: true }
                );
                
                if (!lead.name && contact?.profile?.name) {
                    lead = await Lead.findOneAndUpdate(
                        { phone },
                        { name: contact.profile.name },
                        { new: true, lean: true }
                    );
                }

                // Emite para o cliente IMEDIATAMENTE (a mídia chega depois)
                io.to(phone).emit('newMessage', {
                    _id: savedMsg._id, phone: savedMsg.phone, text: savedMsg.text,
                    direction: savedMsg.direction, type: savedMsg.type,
                    buttonPayload: savedMsg.buttonPayload || null,
                    reactionToWamid: savedMsg.reactionToWamid || null,
                    reactionEmoji: savedMsg.reactionEmoji || null,
                    mediaData: null, timestamp: savedMsg.timestamp,
                    status: savedMsg.status, wamid: savedMsg.wamid, fileName: savedMsg.fileName,
                    locationData: savedMsg.locationData || null
                });
                
                io.emit('conversationUpdate', {
                    phone, name: lead?.name || phone, lastMessage: text,
                    timestamp: savedMsg.timestamp, lastReceived: savedMsg.timestamp,
                    tags: lead?.tags || [], direction: 'received'
                });

                // Baixa a mídia em background e emite atualização separada
                if (isMedia) {
                    const mediaId = msg[type]?.id;
                    if (mediaId) {
                        setImmediate(async () => {
                            try {
                                const mediaRes = await axios.get(
                                    `https://graph.facebook.com/${META_GRAPH_API_VERSION}/${mediaId}`,
                                    { headers: { Authorization: `Bearer ${config.metaAccessToken}` }, timeout: 8000 }
                                );
                                const fileRes = await axios.get(
                                    mediaRes.data.url,
                                    { headers: { Authorization: `Bearer ${config.metaAccessToken}` }, responseType: 'arraybuffer', timeout: 10000 }
                                );
                                const buffer = Buffer.from(fileRes.data);
                                const responseMime = mediaRes.data.mime_type || fileRes.headers?.['content-type'] || msg[type]?.mime_type;

                                if (type === 'video') {
                                    const mimeType = getSupportedVideoMimeType(responseMime, fileName) || 'video/mp4';
                                    const mediaFileId = await saveVideoToGridFS(
                                        buffer,
                                        fileName || `video-${savedMsg._id}.${mimeType === 'video/3gpp' ? '3gp' : 'mp4'}`,
                                        mimeType
                                    );
                                    await Message.findByIdAndUpdate(savedMsg._id, {
                                        mediaFileId,
                                        mediaMimeType: mimeType,
                                        metaMediaId: null
                                    });
                                    io.to(phone).emit('mediaUpdate', {
                                        _id: String(savedMsg._id),
                                        mediaData: `/api/messages/media/${savedMsg._id}/content`,
                                        mediaMimeType: mimeType
                                    });
                                } else {
                                    const mimeType = responseMime || (type === 'audio' ? 'audio/ogg' : (type === 'document' ? 'application/pdf' : 'image/jpeg'));
                                    const mediaData = `data:${mimeType};base64,${buffer.toString('base64')}`;

                                    await Message.findByIdAndUpdate(savedMsg._id, { mediaData, mediaMimeType: mimeType, metaMediaId: null });

                                    // Emite a mídia quando estiver pronta — o frontend atualiza a mensagem
                                    io.to(phone).emit('mediaUpdate', { _id: String(savedMsg._id), mediaData, mediaMimeType: mimeType });
                                }
                            } catch (e) {
                                console.error('Erro download mídia:', e.message);
                            }
                        });
                    }
                }
            }
        } catch (e) {
            console.error('Erro webhook:', e);
        }
    });
});

// ============================================
// LEITURA DE EXCEL — extrai telefones no padrão brasileiro
// ============================================

// Valida e normaliza um valor bruto como telefone brasileiro
function extractBrazilianPhone(raw) {
    if (raw === null || raw === undefined) return null;
    const digits = String(raw).replace(/\D/g, '');
    if (digits.length < 10 || digits.length > 13) return null;
    const normalized = normalizePhone(digits);
    if (!normalized || !normalized.startsWith('55') || normalized.length !== 12) return null;
    return normalized;
}

// Verifica se um valor de célula parece um nome de pessoa
function looksLikeName(raw) {
    if (raw === null || raw === undefined) return false;
    const str = String(raw).trim();
    if (str.length < 2 || str.length > 80) return false;
    // Deve ter letras
    if (!/[a-zA-ZÀ-ÿ]/.test(str)) return false;
    // Não pode ser e-mail, data ou número puro
    if (str.includes('@')) return false;
    if (/^\d{2}[\/-]\d{2}[\/-]\d{2,4}$/.test(str)) return false;
    if (/^\d+$/.test(str)) return false;
    return true;
}

// Detecta índices de colunas de nome e telefone pelo cabeçalho
function detectColumns(headerRow) {
    if (!headerRow) return { nameCol: -1, phoneCol: -1 };
    let nameCol = -1, phoneCol = -1;
    const nameWords = ['nome', 'name', 'cliente', 'contato', 'contact'];
    const phoneWords = ['telefone', 'celular', 'whatsapp', 'phone', 'cel', 'fone', 'numero', 'número', 'tel'];
    headerRow.forEach((cell, i) => {
        const v = String(cell || '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '');
        if (nameCol === -1 && nameWords.some(w => v.includes(w))) nameCol = i;
        if (phoneCol === -1 && phoneWords.some(w => v.includes(w))) phoneCol = i;
    });
    return { nameCol, phoneCol };
}

app.post('/api/excel/parse', (req, res) => {
    try {
        const { excelData } = req.body;
        if (!excelData) return res.status(400).json({ error: 'excelData obrigatório' });

        const base64 = excelData.includes(',') ? excelData.split(',')[1] : excelData;
        const buffer = Buffer.from(base64, 'base64');
        const workbook = XLSX.read(buffer, { type: 'buffer' });

        const seen = new Set();
        const contacts = [];

        for (const sheetName of workbook.SheetNames) {
            const sheet = workbook.Sheets[sheetName];
            const rows = XLSX.utils.sheet_to_json(sheet, { header: 1, raw: true, defval: null });
            if (rows.length === 0) continue;

            // Tenta detectar cabeçalho na primeira linha
            const { nameCol, phoneCol } = detectColumns(rows[0]);
            const hasHeader = nameCol !== -1 || phoneCol !== -1;
            const startRow = hasHeader ? 1 : 0;

            for (let r = startRow; r < rows.length; r++) {
                const row = rows[r];
                if (!row || row.length === 0) continue;

                if (hasHeader && phoneCol !== -1) {
                    // Modo estruturado: usa as colunas detectadas
                    const phone = extractBrazilianPhone(row[phoneCol]);
                    if (phone && !seen.has(phone)) {
                        seen.add(phone);
                        const name = (nameCol !== -1 && looksLikeName(row[nameCol]))
                            ? String(row[nameCol]).trim()
                            : '';
                        contacts.push({ phone, name });
                    }
                } else {
                    // Modo livre: varre a linha inteira buscando telefone e nome
                    let rowPhone = null;
                    let rowName = '';
                    for (const cell of row) {
                        if (!rowPhone) {
                            const p = extractBrazilianPhone(cell);
                            if (p) { rowPhone = p; continue; }
                        }
                        if (!rowName && looksLikeName(cell)) {
                            rowName = String(cell).trim();
                        }
                    }
                    if (rowPhone && !seen.has(rowPhone)) {
                        seen.add(rowPhone);
                        contacts.push({ phone: rowPhone, name: rowName });
                    }
                }
            }
        }

        // Mantém `phones` para compatibilidade com versões anteriores
        res.json({ contacts, phones: contacts.map(c => c.phone), total: contacts.length });
    } catch (e) {
        console.error('Erro ao parsear Excel:', e.message);
        res.status(500).json({ error: 'Erro ao processar arquivo: ' + e.message });
    }
});

// ============================================
// UPLOAD DE MÍDIA (para disparo em massa: faz upload uma vez, reutiliza mediaId)
// ============================================
app.post('/api/media/upload', async (req, res) => {
    try {
        const { mediaData, mediaType, fileName } = req.body;
        if (!mediaData) return res.status(400).json({ error: 'mediaData obrigatório' });
        const config = await getConfig();
        if (!config?.metaAccessToken) return res.status(400).json({ error: 'Config incompleta' });

        const buffer = Buffer.from(mediaData.split(',')[1], 'base64');
        const formData = new FormData();
        formData.append('file', buffer, { filename: fileName || 'file', contentType: mediaType });
        formData.append('messaging_product', 'whatsapp');

        const uploadRes = await axios.post(
            `https://graph.facebook.com/${META_GRAPH_API_VERSION}/${config.metaPhoneNumberId}/media`,
            formData,
            { headers: { ...formData.getHeaders(), Authorization: `Bearer ${config.metaAccessToken}` }, timeout: 30000 }
        );
        res.json({ mediaId: uploadRes.data.id });
    } catch (e) {
        console.error('Erro upload mídia:', e.response?.data || e.message);
        res.status(500).json({ error: e.response?.data?.error?.message || e.message });
    }
});

// ============================================
// ENVIO DE MENSAGEM
// ============================================
app.post('/api/messages/send', async (req, res) => {
    let claimedCampaignRecipient = null;
    let metaAccepted = false;
    const sendStartedAt = Date.now();
    let sendStage = 'configuracao';
    const sendLabel = String(req.body?.phone || '').replace(/\D/g, '').slice(-4);
    const markSendStage = stage => {
        sendStage = stage;
        console.log(`[Dispatch:***${sendLabel}:${stage}] ${Date.now() - sendStartedAt}ms`);
    };
    markSendStage('configuracao');
    res.once('finish', () => console.log(`[Dispatch:***${sendLabel}:resposta] HTTP ${res.statusCode}; etapa=${sendStage}; ${Date.now() - sendStartedAt}ms`));
    res.once('close', () => {
        if (!res.writableFinished) console.warn(`[Dispatch:***${sendLabel}:conexao-encerrada] etapa=${sendStage}; ${Date.now() - sendStartedAt}ms; confira o resultado antes de repetir`);
    });
    try {
        let { phone, messageText, templateName, languageCode, mediaData, mediaType, fileName, mediaId, replyToWamid, lat, lng, campaignRecipientId } = req.body;
        phone = normalizePhone(phone);
        let videoBufferForStorage = null;
        let videoMimeTypeForStorage = null;
        let videoMediaIdForStorage = null;
        
        // Usa cache — não vai ao banco em cada envio
        const config = await getConfig();
        if (!config?.metaAccessToken || !config?.metaPhoneNumberId) {
            return res.status(400).json({ error: 'Configuração da Meta incompleta: confira o token e o identificador do número em Configurações.' });
        }
        markSendStage('validar-destinatario');

        if (campaignRecipientId) {
            if (!templateName || !mongoose.Types.ObjectId.isValid(campaignRecipientId)) {
                return res.status(400).json({ error: 'Campanha ou template inválido.' });
            }
            const recipient = await RemarketingRecipient.findById(campaignRecipientId)
                .select('phone status')
                .lean();
            if (!recipient || recipient.phone !== phone) {
                return res.status(400).json({ error: 'O contato não pertence a esta campanha.' });
            }
            claimedCampaignRecipient = await RemarketingRecipient.findOneAndUpdate(
                { _id: campaignRecipientId, status: 'pending' },
                { $set: { status: 'sending', startedAt: new Date() }, $inc: { attempts: 1 } },
                { new: true }
            );
            if (!claimedCampaignRecipient) {
                return res.status(409).json({ error: 'Este contato já foi iniciado ou enviado nesta rodada.' });
            }
        }

        // ── Envio de Localização ──────────────────────────────
        if (lat !== undefined && lng !== undefined) {
            const locationPayload = {
                messaging_product: "whatsapp",
                to: phone,
                type: "location",
                location: {
                    latitude: lat,
                    longitude: lng
                }
            };
            const metaRes = await axios.post(
                `https://graph.facebook.com/${META_GRAPH_API_VERSION}/${config.metaPhoneNumberId}/messages`,
                locationPayload,
                { headers: { Authorization: `Bearer ${config.metaAccessToken}` }, timeout: 10000 }
            );
            const wamid = metaRes.data.messages?.[0]?.id;
            res.json({ success: true, wamid });
            setImmediate(async () => {
                try {
                    const [savedMsg, lead] = await Promise.all([
                        Message.create({
                            phone,
                            text: '📍 Localização compartilhada',
                            direction: 'sent',
                            type: 'location',
                            wamid,
                            status: 'sent',
                            locationData: { lat, lng, name: null, address: null }
                        }),
                        Lead.findOneAndUpdate({ phone }, { updatedAt: new Date() }, { new: true, lean: true })
                    ]);
                    io.to(phone).emit('newMessage', { ...savedMsg.toObject(), locationData: savedMsg.locationData });
                    io.emit('conversationUpdate', { phone, name: lead?.name || phone, lastMessage: '📍 Localização compartilhada', timestamp: savedMsg.timestamp, direction: 'sent' });
                } catch (e) {
                    console.error('Erro ao salvar localização enviada:', e.message);
                }
            });
            return;
        }

        let payload = { messaging_product: "whatsapp", to: phone };
        if (replyToWamid) payload.context = { message_id: replyToWamid };
        let finalText = messageText || '';
        let finalType = 'text';

        if (templateName) {
            payload.type = "template";
            payload.template = { name: templateName, language: { code: languageCode || "pt_BR" } };

            // mediaId já carregado previamente (disparo em massa) ou carrega agora
            if (mediaId || mediaData) {
                let uploadedId = mediaId;
                if (!uploadedId) {
                    const buffer = Buffer.from(mediaData.split(',')[1], 'base64');
                    const formData = new FormData();
                    formData.append('file', buffer, { filename: fileName || 'file', contentType: mediaType });
                    formData.append('messaging_product', 'whatsapp');
                    const uploadRes = await axios.post(
                        `https://graph.facebook.com/${META_GRAPH_API_VERSION}/${config.metaPhoneNumberId}/media`,
                        formData,
                        { headers: { ...formData.getHeaders(), Authorization: `Bearer ${config.metaAccessToken}` }, timeout: 20000 }
                    );
                    uploadedId = uploadRes.data.id;
                }
                const mediaTypeKey = mediaType?.includes('image') ? 'image' : (mediaType?.includes('pdf') ? 'document' : 'image');
                payload.template.components = [{ type: "header", parameters: [{ type: mediaTypeKey, [mediaTypeKey]: { id: uploadedId } }] }];
            }
        } else if (mediaData) {
            const buffer = getDataUrlBuffer(mediaData);
            const supportedVideoMime = getSupportedVideoMimeType(mediaType, fileName);
            const isVideo = String(mediaType || '').toLowerCase().startsWith('video/') || Boolean(supportedVideoMime);
            if (isVideo) {
                if (!supportedVideoMime) {
                    return res.status(400).json({ error: 'Vídeo não compatível. Use MP4 ou 3GP.' });
                }
                if (buffer.length > MAX_META_VIDEO_BYTES) {
                    return res.status(413).json({ error: 'O vídeo deve ter no máximo 16 MB.' });
                }
            }

            const formData = new FormData();
            formData.append('file', buffer, {
                filename: fileName || (isVideo ? 'video.mp4' : 'file'),
                contentType: isVideo ? supportedVideoMime : (mediaType || 'application/octet-stream')
            });
            formData.append('messaging_product', 'whatsapp');

            const uploadRes = await axios.post(
                `https://graph.facebook.com/${META_GRAPH_API_VERSION}/${config.metaPhoneNumberId}/media`,
                formData,
                { headers: { ...formData.getHeaders(), Authorization: `Bearer ${config.metaAccessToken}` }, timeout: 20000 }
            );

            finalType = isVideo
                ? 'video'
                : (mediaType?.includes('audio') ? 'audio' : (mediaType?.includes('pdf') || mediaType?.includes('word') || mediaType?.includes('document') ? 'document' : 'image'));
            payload.type = finalType;
            payload[finalType] = { id: uploadRes.data.id };
            // Adiciona caption/filename sem sobrescrever o id do upload
            if (finalType === 'image' && messageText) payload.image.caption = messageText;
            if (finalType === 'video' && messageText) payload.video.caption = messageText;
            if (finalType === 'document') payload.document = { id: uploadRes.data.id, filename: fileName || 'arquivo' };
            finalText = messageText || `[${finalType.toUpperCase()}]`;
            if (finalType === 'video') {
                videoBufferForStorage = buffer;
                videoMimeTypeForStorage = supportedVideoMime;
                videoMediaIdForStorage = uploadRes.data.id;
            }
        } else {
            payload.type = "text";
            payload.text = { body: messageText };
        }

        markSendStage('enviar-meta');
        const metaRes = await axios.post(
            `https://graph.facebook.com/${META_GRAPH_API_VERSION}/${config.metaPhoneNumberId}/messages`,
            payload,
            { headers: { Authorization: `Bearer ${config.metaAccessToken}` }, timeout: 10000 }
        );

        const wamid = metaRes.data.messages?.[0]?.id;
        metaAccepted = true;
        markSendStage('registrar-aceite');

        if (claimedCampaignRecipient) {
            await setRemarketingRecipientStatus(claimedCampaignRecipient._id, 'sent', {
                wamid: wamid || null,
                sentAt: new Date(),
                error: null
            });
        }

        // Responde ao frontend imediatamente após Meta confirmar — não espera o banco
        res.json({ success: true, wamid });

        // Salva no banco e emite socket em background (não bloqueia a resposta)
        setImmediate(async () => {
            try {
                let mediaFileId = null;
                if (finalType === 'video' && videoBufferForStorage) {
                    try {
                        mediaFileId = await saveVideoToGridFS(
                            videoBufferForStorage,
                            fileName || 'video.mp4',
                            videoMimeTypeForStorage
                        );
                    } catch (storageError) {
                        console.error('Erro ao armazenar vídeo enviado:', storageError.message);
                    }
                }
                const [savedMsg, lead] = await Promise.all([
                    Message.findOneAndUpdate(
                        wamid ? { wamid, direction: 'sent' } : { _id: new mongoose.Types.ObjectId() },
                        {
                            $set: {
                                phone, text: finalText, direction: 'sent', type: finalType,
                                mediaData: finalType === 'video' ? null : (mediaData || null),
                                mediaFileId,
                                mediaMimeType: finalType === 'video' ? videoMimeTypeForStorage : null,
                                metaMediaId: finalType === 'video' ? videoMediaIdForStorage : null,
                                ...(wamid ? { wamid } : {}), fileName
                            },
                            // Não apaga falha/entrega recebida antes deste salvamento.
                            $setOnInsert: { status: 'sent', timestamp: new Date() }
                        },
                        { upsert: true, new: true }
                    ),
                    Lead.findOneAndUpdate({ phone }, { updatedAt: new Date() }, { new: true, lean: true })
                ]);
                const savedMessage = savedMsg.toObject();
                if (finalType === 'video') {
                    savedMessage.mediaData = `/api/messages/media/${savedMsg._id}/content`;
                }
                io.to(phone).emit('newMessage', savedMessage);
                io.emit('conversationUpdate', { phone, name: lead?.name || phone, lastMessage: finalText, timestamp: savedMsg.timestamp, direction: 'sent' });
            } catch (e) {
                console.error('Erro ao salvar msg enviada:', e.message);
            }
        });

    } catch (e) {
        const metaError = e.response?.data?.error;
        const errorText = metaError
            ? `[${metaError.code || 'Meta'}] ${metaError.message || 'Envio recusado'}${metaError.error_data?.details ? ' — ' + metaError.error_data.details : ''}`
            : e.message;
        const failureStatus = metaAccepted || !e.response || e.response.status >= 500 ? 'uncertain' : 'failed';
        if (claimedCampaignRecipient) {
            await setRemarketingRecipientStatus(
                claimedCampaignRecipient._id,
                failureStatus,
                { error: String(errorText || 'Falha no envio').slice(0, 2000) }
            ).catch(statusError => console.error('Erro ao atualizar campanha após falha:', statusError.message));
        }
        // Recusas diretas também precisam ficar no histórico após fechar a tela.
        // Nunca registra timeout/erro interno como falha confirmada da Meta.
        if (!claimedCampaignRecipient && req.body?.templateName && metaError && failureStatus === 'failed') {
            try {
                await Message.create({
                    phone: normalizePhone(req.body.phone), text: req.body.messageText || '[Oferta recusada pela Meta]',
                    direction: 'sent', type: 'text', status: 'failed',
                    error: String(errorText || 'Envio recusado').slice(0, 2000)
                });
            } catch (saveError) {
                console.error('Erro ao guardar recusa da Meta:', saveError.message);
            }
        }
        console.error(`[Dispatch:***${sendLabel}:erro] etapa=${sendStage}; resultado=${failureStatus}; ${Date.now() - sendStartedAt}ms; ${errorText}`);
        const metaHttpStatus = e.response?.status;
        const responseStatus = failureStatus === 'failed'
            ? ([401, 403, 429].includes(metaHttpStatus) ? metaHttpStatus : 400)
            : 500;
        res.status(responseStatus).json({ error: errorText, outcome: failureStatus });
    }
});

// Filas derivadas do histórico existente, sem alterar tags nem reenviar mensagens.
// O último envio prevalece; uma resposta posterior devolve o contato à triagem.
const deliveryQueueCache = { data: null, at: 0, pending: null };
async function loadDeliveryQueueSnapshot() {
        // Primeiro enumera telefones pelo índice direction/phone/timestamp.
        // Só então lê a última mensagem de cada telefone. Filtrar type e ler
        // status/error antes de agrupar obrigava a buscar todo o histórico de
        // mensagens (inclusive mídia em base64) a cada atualização da tela.
        const latestMessagePipeline = direction => [
            { $match: { direction } },
            { $sort: { phone: 1 } },
            { $group: { _id: '$phone' } },
            { $lookup: {
                from: Message.collection.name,
                let: { phone: '$_id' },
                pipeline: [
                    { $match: {
                        $expr: { $and: [
                            { $eq: ['$phone', '$$phone'] },
                            { $eq: ['$direction', direction] }
                        ] },
                        type: { $ne: 'reaction' }
                    } },
                    { $sort: { timestamp: -1 } },
                    { $limit: 1 },
                    { $project: { status: 1, error: 1, timestamp: 1 } }
                ],
                as: 'latest'
            } },
            { $unwind: '$latest' },
            { $project: { _id: 1, status: '$latest.status', error: '$latest.error', at: '$latest.timestamp' } }
        ];
        const queryOptions = { maxTimeMS: 20000, allowDiskUse: true };
        const runStage = async (stage, query) => {
            const startedAt = Date.now();
            try {
                return await query;
            } catch (error) {
                console.error(`[DeliveryQueue:${stage}] Consulta falhou após ${Date.now() - startedAt}ms`, {
                    code: error.code, codeName: error.codeName, message: error.message
                });
                throw error;
            }
        };
        const [messages, recipients, replies] = await Promise.all([
            runStage('ultimos-envios', Message.aggregate(latestMessagePipeline('sent')).option(queryOptions)),
            runStage('tentativas-campanhas', RemarketingRecipient.aggregate([
                { $match: { status: { $ne: 'pending' } } },
                // $max compara primeiro a data e depois o id, sem ordenar todos
                // os destinatários por uma data calculada fora de um índice.
                { $group: { _id: '$phone', latest: { $max: {
                    at: { $ifNull: ['$startedAt', { $ifNull: ['$sentAt', '$createdAt'] }] },
                    id: '$_id', status: '$status', error: '$error'
                } } } },
                { $project: { _id: 1, status: '$latest.status', error: '$latest.error', at: '$latest.at' } }
            ]).option(queryOptions)),
            runStage('ultimas-respostas', Message.aggregate(latestMessagePipeline('received')).option(queryOptions))
        ]);
        const latest = new Map(messages.map(row => [row._id, row]));
        for (const row of recipients) {
            const current = latest.get(row._id);
            // O registro da mensagem traz a confirmação de entrega; não sobrescreve
            // esse retorno com o estado 'sent' da tentativa da mesma campanha.
            if (!current || new Date(row.at) > new Date(current.at)) latest.set(row._id, row);
        }
        const replyByPhone = new Map(replies.map(row => [row._id, row.at]));
        const contacts = latest.size ? await runStage('nomes-contatos',
            Lead.find({ phone: { $in: [...latest.keys()] } }, null, { maxTimeMS: 20000 })
                .select('phone name tags').lean()
        ) : [];
        const byPhone = new Map(contacts.map(row => [row.phone, row]));
        const rows = [...latest.values()].map(row => {
            const replied = new Date(replyByPhone.get(row._id) || 0) > new Date(row.at);
            const confirmed = ['delivered', 'read'].includes(row.status);
            return { phone: row._id, name: byPhone.get(row._id)?.name || row._id,
                tags: byPhone.get(row._id)?.tags || [], status: row.status,
                error: row.error || '', timestamp: row.at,
                queue: replied || confirmed ? 'triage' : row.status === 'failed' ? 'failed' : 'unconfirmed' };
        }).sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
        return { rows, syncedAt: new Date().toISOString() };
}

app.get('/api/messages/delivery-queue', async (req, res) => {
    try {
        // Abas e usuários simultâneos compartilham a mesma consulta. Somente
        // resultados completos são guardados; uma falha nunca vira fila vazia.
        if (deliveryQueueCache.data && Date.now() - deliveryQueueCache.at < 10000) {
            return res.json(deliveryQueueCache.data);
        }
        if (!deliveryQueueCache.pending) {
            deliveryQueueCache.pending = loadDeliveryQueueSnapshot().then(data => {
                deliveryQueueCache.data = data;
                deliveryQueueCache.at = Date.now();
                return data;
            }).finally(() => { deliveryQueueCache.pending = null; });
        }
        res.json(await deliveryQueueCache.pending);
    } catch (error) {
        console.error('Erro ao separar filas de entrega:', error.message);
        const timedOut = error.code === 50 || error.codeName === 'MaxTimeMSExpired';
        const unavailable = ['MongoNetworkError', 'MongoServerSelectionError', 'MongooseServerSelectionError'].includes(error.name);
        res.status(timedOut || unavailable ? 503 : 500).json({
            code: timedOut ? 'DELIVERY_QUERY_TIMEOUT' : unavailable ? 'DELIVERY_DATABASE_UNAVAILABLE' : 'DELIVERY_QUERY_FAILED',
            error: timedOut
                ? 'O MongoDB excedeu o tempo limite ao consultar o histórico de entregas. Verifique os logs do Railway e os índices do banco.'
                : unavailable
                    ? 'O servidor não conseguiu conectar ao MongoDB. Verifique a conexão do banco no Railway.'
                    : 'O servidor falhou ao consultar o histórico de entregas. Consulte o erro nos logs do Railway.',
            // Nunca devolve rows: [] como se uma consulta com erro tivesse sincronizado.
            retryable: true
        });
    }
});

// ============================================
// ROTAS DE CONVERSAS - SIMPLES E RÁPIDO
// ============================================

let conversationsCache = { data: null, timestamp: 0, ttl: 15000 };

app.get("/api/conversations", async (req, res) => {
    const startTime = Date.now();
    
    if (conversationsCache.data && (Date.now() - conversationsCache.timestamp) < conversationsCache.ttl) {
        return res.json(conversationsCache.data);
    }

    const timeoutId = setTimeout(() => {
        if (!res.headersSent) {
            console.warn('[Conversations] Timeout — retornando cache');
            res.json(conversationsCache.data || []);
        }
    }, 26000);

    try {
        if (mongoose.connection.readyState !== 1) {
            clearTimeout(timeoutId);
            return res.json(conversationsCache.data || []);
        }

        // Use indexes ordered by phone and timestamp, instead of sorting every
        // message in the collection by timestamp before grouping.
        const [latestMessages, latestReceivedMessages] = await Promise.all([
            Message.aggregate([
                { $sort: { phone: 1, timestamp: -1 } },
                { $group: {
                    _id: '$phone',
                    lastMessage: { $first: '$text' },
                    direction: { $first: '$direction' },
                    timestamp: { $first: '$timestamp' }
                }},
                { $sort: { timestamp: -1 } }
            ]).option({ maxTimeMS: 20000 }),
            Message.aggregate([
                { $match: { direction: 'received' } },
                { $sort: { phone: 1, timestamp: -1 } },
                { $group: {
                    _id: '$phone',
                    lastReceived: { $first: '$timestamp' }
                }}
            ]).option({ maxTimeMS: 20000 })
        ]);

        clearTimeout(timeoutId);

        const lastReceivedByPhone = new Map(
            latestReceivedMessages.map(message => [message._id, message.lastReceived])
        );
        const agg = latestMessages.map(message => ({
            ...message,
            lastReceived: lastReceivedByPhone.get(message._id) || null
        }));
        const phones = agg.map(a => a._id);

        let leadsMap = new Map();
        if (phones.length > 0) {
            try {
                const leads = await Lead.find({ phone: { $in: phones } })
                    .select('phone name tags')
                    .maxTimeMS(5000)
                    .lean();
                leadsMap = new Map(leads.map(l => [l.phone, l]));
            } catch (e) {
                console.error('[Conversations] Erro leads:', e.message);
            }
        }

        const result = agg.map(c => {
            const lead = leadsMap.get(c._id);
            return {
                phone: c._id,
                name: lead?.name || c._id,
                lastMessage: c.lastMessage || '',
                lastDirection: c.direction,
                timestamp: c.timestamp,
                lastReceived: c.lastReceived || null,
                tags: lead?.tags || []
            };
        });

        conversationsCache = { data: result, timestamp: Date.now(), ttl: 15000 };
        console.log(`[Conversations] ${result.length} conversas em ${Date.now() - startTime}ms`);
        res.json(result);

    } catch (error) {
        clearTimeout(timeoutId);
        console.error('[Conversations] ERRO:', error.message);
        res.json(conversationsCache.data || []);
    }
});

app.post('/api/messages/forward-video', async (req, res) => {
    try {
        const sourceMessageId = String(req.body?.messageId || '');
        const phone = normalizePhone(String(req.body?.phone || '').trim());
        if (!sourceMessageId || !phone) {
            return res.status(400).json({ error: 'Mensagem e número de destino são obrigatórios.' });
        }

        const source = await Message.findById(sourceMessageId)
            .select('phone text type mediaData mediaFileId mediaMimeType metaMediaId fileName')
            .lean();
        if (!source || source.type !== 'video') {
            return res.status(404).json({ error: 'Vídeo não encontrado.' });
        }

        const config = await getConfig();
        if (!config?.metaAccessToken || !config?.metaPhoneNumberId) {
            return res.status(400).json({ error: 'Configuração da Meta incompleta.' });
        }

        let buffer;
        let sourceMimeType = source.mediaMimeType;
        if (source.mediaFileId) {
            buffer = await readVideoFromGridFS(source.mediaFileId);
        } else if (source.mediaData) {
            buffer = getDataUrlBuffer(source.mediaData);
            const dataUrlMime = String(source.mediaData).match(/^data:([^;]+);base64,/i)?.[1];
            sourceMimeType = sourceMimeType || dataUrlMime;
        } else if (source.metaMediaId) {
            const downloaded = await downloadMetaMedia(source.metaMediaId, config);
            buffer = downloaded.buffer;
            sourceMimeType = sourceMimeType || downloaded.mimeType;
        } else {
            return res.status(404).json({ error: 'O arquivo do vídeo não está disponível para reenvio.' });
        }

        if (buffer.length > MAX_META_VIDEO_BYTES) {
            return res.status(413).json({ error: 'O vídeo excede o limite de 16 MB da Meta.' });
        }
        // Vídeos recebidos por versões anteriores podiam estar salvos com MIME image/jpeg.
        const videoMimeType = getSupportedVideoMimeType(sourceMimeType, source.fileName) || 'video/mp4';
        const fileName = source.fileName || `video.${videoMimeType === 'video/3gpp' ? '3gp' : 'mp4'}`;
        const formData = new FormData();
        formData.append('file', buffer, { filename: fileName, contentType: videoMimeType });
        formData.append('messaging_product', 'whatsapp');

        const uploadRes = await axios.post(
            `https://graph.facebook.com/${META_GRAPH_API_VERSION}/${config.metaPhoneNumberId}/media`,
            formData,
            { headers: { ...formData.getHeaders(), Authorization: `Bearer ${config.metaAccessToken}` }, timeout: 30000 }
        );
        const mediaId = uploadRes.data.id;
        const previousCaption = source.text && !/^\[VIDEO\]$/i.test(source.text) ? source.text : '';
        const caption = String(req.body?.caption ?? previousCaption).slice(0, 1024);
        const payload = {
            messaging_product: 'whatsapp',
            to: phone,
            type: 'video',
            video: { id: mediaId }
        };
        if (caption) payload.video.caption = caption;

        const metaRes = await axios.post(
            `https://graph.facebook.com/${META_GRAPH_API_VERSION}/${config.metaPhoneNumberId}/messages`,
            payload,
            { headers: { Authorization: `Bearer ${config.metaAccessToken}` }, timeout: 15000 }
        );
        const wamid = metaRes.data.messages?.[0]?.id;
        res.json({ success: true, wamid });

        setImmediate(async () => {
            try {
                let mediaFileId = source.mediaFileId || null;
                if (!mediaFileId) {
                    try {
                        mediaFileId = await saveVideoToGridFS(buffer, fileName, videoMimeType);
                    } catch (storageError) {
                        console.error('Erro ao armazenar vídeo encaminhado:', storageError.message);
                    }
                }

                const [savedMsg, lead] = await Promise.all([
                    Message.create({
                        phone,
                        text: caption || '[VIDEO]',
                        direction: 'sent',
                        type: 'video',
                        mediaData: null,
                        mediaFileId,
                        mediaMimeType: videoMimeType,
                        metaMediaId: mediaId,
                        wamid,
                        fileName,
                        status: 'sent'
                    }),
                    Lead.findOneAndUpdate({ phone }, { updatedAt: new Date() }, { new: true, lean: true })
                ]);
                io.to(phone).emit('newMessage', {
                    ...savedMsg.toObject(),
                    mediaData: `/api/messages/media/${savedMsg._id}/content`
                });
                io.emit('conversationUpdate', {
                    phone,
                    name: lead?.name || phone,
                    lastMessage: caption || '[VIDEO]',
                    timestamp: savedMsg.timestamp,
                    direction: 'sent'
                });
            } catch (error) {
                console.error('Erro ao salvar vídeo encaminhado:', error.message);
            }
        });
    } catch (error) {
        console.error('Erro ao encaminhar vídeo:', error.response?.data || error.message);
        res.status(500).json({ error: error.response?.data?.error?.message || error.message });
    }
});

// ============================================
// ROTAS DE MENSAGENS
// ============================================

// Retorna só o campo mediaData de uma mensagem — para o frontend verificar se mídia já baixou
app.get('/api/messages/media/:id', async (req, res) => {
    try {
        const msg = await Message.findById(req.params.id)
            .select('mediaData mediaFileId mediaMimeType metaMediaId type fileName')
            .lean();
        if (!msg) return res.status(404).json({ error: 'Mensagem não encontrada' });
        if (msg.type === 'video' && (msg.mediaFileId || msg.metaMediaId)) {
            return res.json({
                mediaData: `/api/messages/media/${msg._id}/content`,
                type: msg.type,
                mediaMimeType: msg.mediaMimeType || 'video/mp4'
            });
        }
        let mediaData = msg.mediaData;
        if (msg.type === 'video' && mediaData) {
            const match = mediaData.match(/^data:([^;]+);base64,(.+)$/);
            if (match) {
                const mimeType = getSupportedVideoMimeType(msg.mediaMimeType || match[1], msg.fileName) || 'video/mp4';
                mediaData = `data:${mimeType};base64,${match[2]}`;
            }
        }
        res.json({ mediaData, type: msg.type, mediaMimeType: msg.mediaMimeType || null });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

app.get('/api/messages/media/:id/content', async (req, res) => {
    try {
        const msg = await Message.findById(req.params.id)
            .select('mediaData mediaFileId mediaMimeType metaMediaId type fileName')
            .lean();
        if (!msg || msg.type !== 'video') {
            return res.status(404).json({ error: 'Vídeo não encontrado.' });
        }

        const mimeType = getSupportedVideoMimeType(msg.mediaMimeType, msg.fileName) || 'video/mp4';
        if (msg.mediaFileId) {
            return streamGridFsFile(res, msg.mediaFileId, mimeType, msg.fileName || 'video.mp4');
        }

        let buffer;
        if (msg.mediaData) {
            buffer = getDataUrlBuffer(msg.mediaData);
        } else if (msg.metaMediaId) {
            const config = await getConfig();
            if (!config?.metaAccessToken) return res.status(400).json({ error: 'Configuração da Meta incompleta.' });
            const downloaded = await downloadMetaMedia(msg.metaMediaId, config);
            buffer = downloaded.buffer;
            const resolvedMime = getSupportedVideoMimeType(downloaded.mimeType, msg.fileName) || mimeType;
            try {
                const mediaFileId = await saveVideoToGridFS(buffer, msg.fileName || 'video.mp4', resolvedMime);
                await Message.findByIdAndUpdate(msg._id, { mediaFileId, mediaMimeType: resolvedMime, metaMediaId: null });
            } catch (storageError) {
                console.error('Não foi possível guardar cópia permanente do vídeo:', storageError.message);
            }
            res.setHeader('Content-Type', resolvedMime);
            res.setHeader('Content-Length', buffer.length);
            return res.send(buffer);
        } else {
            return res.status(404).json({ error: 'Vídeo ainda não disponível.' });
        }

        res.setHeader('Content-Type', mimeType);
        res.setHeader('Content-Length', buffer.length);
        res.send(buffer);
    } catch (e) {
        console.error('Erro ao carregar vídeo:', e.response?.data || e.message);
        if (!res.headersSent) {
            res.status(e.response?.status || 500).json({
                error: e.response?.data?.error?.message || e.message
            });
        }
    }
});

// ============================================
// DOWNLOAD DE MÍDIA — baixa o arquivo enviado por um cliente (foto, áudio, vídeo, documento)
// ============================================
app.get('/api/messages/:id/download', async (req, res) => {
    try {
        const msg = await Message.findById(req.params.id)
            .select('mediaData mediaFileId mediaMimeType metaMediaId type fileName')
            .lean();
        if (!msg) return res.status(404).json({ error: 'Mensagem não encontrada' });
        const rawName = (msg.fileName && msg.fileName.trim()) || `${msg.type || 'arquivo'}-${msg._id}.mp4`;
        const safeName = rawName.replace(/["\\\r\n]/g, '');

        if (msg.type === 'video' && msg.mediaFileId) {
            return streamGridFsFile(res, msg.mediaFileId, msg.mediaMimeType || 'video/mp4', safeName, 'attachment');
        }

        if (!msg.mediaData && msg.type === 'video' && msg.metaMediaId) {
            const config = await getConfig();
            if (!config?.metaAccessToken) return res.status(400).json({ error: 'Configuração da Meta incompleta.' });
            const downloaded = await downloadMetaMedia(msg.metaMediaId, config);
            const mime = getSupportedVideoMimeType(downloaded.mimeType, msg.fileName) || 'video/mp4';
            const fileBuffer = downloaded.buffer;
            try {
                const mediaFileId = await saveVideoToGridFS(fileBuffer, safeName, mime);
                await Message.findByIdAndUpdate(msg._id, { mediaFileId, mediaMimeType: mime, metaMediaId: null });
            } catch (storageError) {
                console.error('Não foi possível guardar cópia permanente do vídeo:', storageError.message);
            }
            res.setHeader('Content-Type', mime);
            res.setHeader('Content-Disposition', `attachment; filename="${safeName}"`);
            res.setHeader('Content-Length', fileBuffer.length);
            return res.send(fileBuffer);
        }

        if (!msg.mediaData) return res.status(404).json({ error: 'Mídia ainda não disponível' });

        const match = msg.mediaData.match(/^data:([^;]+);base64,(.+)$/);
        if (!match) return res.status(400).json({ error: 'Formato de mídia inválido' });

        const mime = msg.type === 'video'
            ? (getSupportedVideoMimeType(msg.mediaMimeType || match[1], msg.fileName) || 'video/mp4')
            : match[1];
        const buffer = Buffer.from(match[2], 'base64');
        const extFromMime = (mime.split('/')[1] || 'bin').replace(/[^a-z0-9]/gi, '') || 'bin';
        const downloadName = (msg.fileName && msg.fileName.trim()) || `${msg.type || 'arquivo'}-${msg._id}.${extFromMime}`;
        const safeDownloadName = downloadName.replace(/["\\\r\n]/g, '');

        res.setHeader('Content-Type', mime);
        res.setHeader('Content-Disposition', `attachment; filename="${safeDownloadName}"`);
        res.setHeader('Content-Length', buffer.length);
        res.send(buffer);
    } catch (e) {
        console.error('Erro download mídia:', e.message);
        res.status(500).json({ error: e.message });
    }
});

app.get('/api/messages/:phone', async (req, res) => {
    try {
        const normalizedPhone = normalizePhone(req.params.phone);
        const limit = parseInt(req.query.limit) || 50;
        const before = req.query.before;

        const query = { phone: normalizedPhone };
        if (before) query.timestamp = { $lt: new Date(before) };

        // Exclui mediaData do retorno — pode ser centenas de KB por mensagem
        // O frontend carrega a mídia separadamente via /api/messages/media/:id
        const messages = await Message.find(query)
            .select('-mediaData')
            .sort({ timestamp: -1 })
            .limit(limit)
            .lean();

        // Adiciona flag hasMedia para o frontend saber quais mensagens têm mídia no banco
        const result = messages.reverse().map(m => ({
            ...m,
            hasMedia: ['image', 'audio', 'video', 'document'].includes(m.type),
            mediaData: null,
            locationData: m.locationData || null
        }));

        res.json(result);

        setImmediate(() => {
            Message.updateMany(
                { phone: normalizedPhone, direction: 'received', status: { $ne: 'read' } },
                { status: 'read' }
            ).exec();
        });
        
    } catch (e) {
        console.error('Erro messages:', e);
        res.status(500).json({ error: e.message });
    }
});

// ============================================
// ROTAS DE LEADS
// ============================================
app.get('/api/remarketing/campaigns', async (req, res) => {
    try {
        const campaigns = await RemarketingCampaign.find()
            .sort({ createdAt: -1 })
            .limit(500)
            .lean();
        res.json(await getRemarketingCampaignSummaries(campaigns, {
            includeDeliveries: req.query?.summary !== 'dispatch'
        }));
    } catch (e) {
        console.error('Erro ao listar campanhas de remarketing:', e.message);
        res.status(500).json({ error: e.message });
    }
});

app.post('/api/remarketing/campaigns', async (req, res) => {
    const preparationStartedAt = Date.now();
    let preparationStage = 'validacao';
    const markPreparationStage = stage => {
        console.log(`[CampaignPreparation:${preparationStage}] ${Date.now() - preparationStartedAt}ms acumulados`);
        preparationStage = stage;
    };
    try {
        const name = String(req.body?.name || '').trim().slice(0, 100);
        const roundName = String(req.body?.roundName || '').trim().slice(0, 80);
        const roundKey = normalizeRemarketingRound(roundName);
        const templateName = String(req.body?.templateName || '').trim().slice(0, 200);
        const languageCode = String(req.body?.languageCode || 'pt_BR').trim().slice(0, 20);
        const templateText = String(req.body?.templateText || '').slice(0, 4000);
        const tags = Array.isArray(req.body?.tags)
            ? [...new Set(req.body.tags.map(tag => String(tag).trim()).filter(Boolean))].slice(0, 100)
            : [];
        const rawPhones = Array.isArray(req.body?.phones) ? req.body.phones : [];
        const phones = [...new Set(rawPhones
            .map(phone => normalizePhone(String(phone || '')))
            .filter(phone => phone && phone.length >= 8 && phone.length <= 15))];

        if (!name || !roundName || !roundKey || !templateName || phones.length === 0) {
            return res.status(400).json({
                error: 'Informe o nome da campanha, a rodada, o template e pelo menos um telefone válido.'
            });
        }

        markPreparationStage('contatos');
        const leads = await Lead.find({ phone: { $in: phones } })
            .select('phone name tags')
            .lean();
        markPreparationStage('gravar-campanha');
        const campaign = await RemarketingCampaign.create({
            name,
            roundName,
            roundKey,
            tags,
            templateName,
            languageCode,
            templateText,
            headerMediaId: String(req.body?.headerMediaId || '').trim() || null
        });

        try {
            markPreparationStage('indice-antirrepeticao');
            await RemarketingRecipient.collection.createIndex(
                { roundKey: 1, phone: 1 },
                { unique: true }
            );
            markPreparationStage('reservar-destinatarios');
            for (let offset = 0; offset < leads.length; offset += 500) {
                const batch = leads.slice(offset, offset + 500);
                if (!batch.length) continue;
                try {
                    await RemarketingRecipient.bulkWrite(batch.map(lead => ({
                        updateOne: {
                            filter: { roundKey, phone: lead.phone },
                            update: {
                                $setOnInsert: {
                                    campaignId: campaign._id,
                                    roundKey,
                                    phone: lead.phone,
                                    name: lead.name || '',
                                    tags: Array.isArray(lead.tags) ? lead.tags : [],
                                    status: 'pending',
                                    attempts: 0
                                }
                            },
                            upsert: true
                        }
                    })), { ordered: false });
                } catch (writeError) {
                    const errors = writeError.writeErrors || [];
                    const onlyDuplicateConflicts = errors.length > 0
                        && errors.every(error => error.code === 11000);
                    if (!onlyDuplicateConflicts && writeError.code !== 11000) throw writeError;
                }
            }

            markPreparationStage('contar-reservados');
            const totalRecipients = await RemarketingRecipient.countDocuments({ campaignId: campaign._id });
            markPreparationStage('responder');
            const skippedCount = phones.length - totalRecipients;
            if (totalRecipients === 0) {
                await RemarketingCampaign.deleteOne({ _id: campaign._id });
                return res.status(409).json({
                    error: 'Nenhum contato novo: todos os números selecionados já estão em uma campanha desta rodada.',
                    skippedCount
                });
            }

            // Todos os destinatários desta campanha acabaram de ser reservados como
            // pendentes. Não cruza o histórico de mensagens antes do primeiro envio:
            // esse relatório é necessário no histórico, não nesta preparação.
            res.status(201).json({
                ...campaign.toObject(),
                totalRecipients,
                pendingCount: totalRecipients,
                sendingCount: 0,
                sentCount: 0,
                failedCount: 0,
                uncertainCount: 0,
                deliveryFailedCount: 0,
                deliveredCount: 0,
                readCount: 0,
                unconfirmedCount: 0,
                estimatedTotalCost: totalRecipients * REMARKETING_COST_PER_MESSAGE,
                estimatedSentCost: 0,
                state: 'draft',
                skippedCount
            });
        } catch (error) {
            await RemarketingRecipient.deleteMany({ campaignId: campaign._id }).catch(() => {});
            await RemarketingCampaign.deleteOne({ _id: campaign._id }).catch(() => {});
            throw error;
        }
    } catch (e) {
        console.error(`[CampaignPreparation:${preparationStage}] Falha após ${Date.now() - preparationStartedAt}ms:`, e.message);
        res.status(500).json({
            code: 'CAMPAIGN_PREPARATION_FAILED',
            stage: preparationStage,
            error: `Não foi possível preparar a campanha (etapa: ${preparationStage}). ${e.message || 'Confira os logs do servidor.'}`
        });
    }
});

app.get('/api/remarketing/campaigns/:id/recipients', async (req, res) => {
    const startedAt = Date.now();
    console.log(`[Dispatch:pendentes:inicio] campanha=${req.params.id}`);
    try {
        if (!mongoose.Types.ObjectId.isValid(req.params.id)) {
            return res.status(400).json({ error: 'Campanha inválida.' });
        }
        const campaignId = new mongoose.Types.ObjectId(req.params.id);
        const status = String(req.query.status || 'pending');
        if (!['pending', 'failed', 'sending', 'uncertain', 'sent'].includes(status)) {
            return res.status(400).json({ error: 'Status de contatos inválido.' });
        }
        const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 500, 1), 1000);
        const recipients = await RemarketingRecipient.find({ campaignId, status })
            .select('phone name status error')
            .sort({ createdAt: 1, _id: 1 })
            .limit(limit)
            .maxTimeMS(15000)
            .lean();
        console.log(`[Dispatch:pendentes:resultado] campanha=${req.params.id}; status=${status}; contatos=${recipients.length}; ${Date.now() - startedAt}ms`);
        res.setHeader('Cache-Control', 'no-store');
        res.json({ recipients });
    } catch (e) {
        console.error(`[Dispatch:pendentes:erro] campanha=${req.params.id}; ${Date.now() - startedAt}ms; ${e.message}`);
        const timedOut = e.code === 50 || e.codeName === 'MaxTimeMSExpired';
        res.status(timedOut ? 503 : 500).json({ error: timedOut
            ? 'A consulta dos contatos pendentes excedeu o limite do banco. Nenhum novo envio foi iniciado nesta consulta; confira Campanhas salvas antes de retomar.'
            : e.message });
    }
});

// Relatório usa somente campos já existentes: destinatários + retorno do webhook por wamid.
app.get('/api/remarketing/campaigns/:id/delivery-report', async (req, res) => {
    try {
        if (!mongoose.Types.ObjectId.isValid(req.params.id)) return res.status(400).json({ error: 'Campanha inválida.' });
        const filter = String(req.query.filter || 'failed');
        const statuses = {
            all: null, failed: ['failed'], unconfirmed: ['accepted', 'uncertain'],
            pending: ['pending'], delivered: ['delivered', 'read']
        };
        if (!Object.prototype.hasOwnProperty.call(statuses, filter)) return res.status(400).json({ error: 'Filtro inválido.' });
        const page = Math.max(parseInt(req.query.page, 10) || 1, 1);
        const limit = 200;
        const pipeline = [
            { $match: { campaignId: new mongoose.Types.ObjectId(req.params.id) } },
            ...getCampaignDeliveryStages(),
            { $facet: {
                summary: [{ $group: { _id: '$deliveryStatus', count: { $sum: 1 } } }],
                counts: [
                    ...(statuses[filter] ? [{ $match: { deliveryStatus: { $in: statuses[filter] } } }] : []),
                    { $count: 'total' }
                ],
                recipients: [
                    ...(statuses[filter] ? [{ $match: { deliveryStatus: { $in: statuses[filter] } } }] : []),
                    { $sort: { createdAt: 1, _id: 1 } }, { $skip: (page - 1) * limit }, { $limit: limit },
                    { $project: { phone: 1, name: 1, wamid: 1, deliveryStatus: 1, deliveryError: 1, sentAt: 1 } }
                ]
            } }
        ];
        const [report] = await RemarketingRecipient.aggregate(pipeline);
        const summary = Object.fromEntries((report?.summary || []).map(row => [row._id, row.count]));
        res.json({ recipients: report?.recipients || [], total: report?.counts?.[0]?.total || 0, summary, page, limit });
    } catch (error) {
        res.status(500).json({ error: 'Não foi possível consultar as entregas: ' + error.message });
    }
});

app.post('/api/messages/delivery-report', async (req, res) => {
    try {
        const ids = [...new Set((Array.isArray(req.body.wamids) ? req.body.wamids : [])
            .filter(id => typeof id === 'string' && id.length < 500))];
        if (ids.length > 200) return res.status(400).json({ error: 'Consulte até 200 mensagens por vez.' });
        const messages = await Message.find({ wamid: { $in: ids }, direction: 'sent' })
            .select('wamid status error').lean();
        // Mesma precedência do relatório de campanhas, inclusive para duplicatas antigas.
        const rank = { read: 4, delivered: 3, failed: 2 };
        const byId = new Map();
        for (const message of messages) {
            const previous = byId.get(message.wamid);
            if (!previous || (rank[message.status] || 1) > (rank[previous.status] || 1)
                || (message.status === previous.status && !previous.error && message.error)) {
                byId.set(message.wamid, message);
            }
        }
        res.json({ messages: [...byId.values()] });
    } catch (error) {
        res.status(500).json({ error: 'Não foi possível atualizar as entregas.' });
    }
});

app.post('/api/remarketing/campaigns/:id/retry-failed', async (req, res) => {
    try {
        if (!mongoose.Types.ObjectId.isValid(req.params.id)) {
            return res.status(400).json({ error: 'Campanha inválida.' });
        }
        const result = await RemarketingRecipient.updateMany(
            { campaignId: req.params.id, status: 'failed' },
            { $set: { status: 'pending', error: null }, $unset: { startedAt: 1 } }
        );
        res.json({ success: true, retriedCount: result.modifiedCount });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

app.get('/api/leads', async (req, res) => {
    try {
        const page = Math.max(parseInt(req.query.page, 10) || 1, 1);
        const requestedLimit = parseInt(req.query.limit, 10);
        const hasPagination = Number.isFinite(requestedLimit) && requestedLimit > 0;
        const limit = hasPagination ? Math.min(requestedLimit, 5000) : 0;
        const query = Lead.find().sort({ updatedAt: -1 });

        // Sem limit por padrão: o painel precisa enxergar todos os leads.
        // Paginação continua disponível para clientes que enviarem page/limit.
        if (hasPagination) {
            query.skip((page - 1) * limit).limit(limit);
        }

        const leads = await query.lean();
        res.json(leads);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

app.post("/api/leads", async (req, res) => {
    try {
        const phone = normalizePhone(String(req.body?.phone || '').trim());
        const name = String(req.body?.name || '').trim();
        if (!name || !phone || phone.length < 8 || phone.length > 15) {
            return res.status(400).json({ error: 'Informe nome e telefone válidos.' });
        }

        // Não apaga tags existentes quando uma edição chega sem tags.
        // Para remover todas de propósito, o cliente deve enviar clearTags: true.
        const update = buildLeadUpdate({
            phone,
            name,
            tags: req.body.tags,
            clearTags: req.body.clearTags
        });

        const lead = await Lead.findOneAndUpdate(
            { phone },
            update,
            { upsert: true, new: true, lean: true }
        );
        io.emit('leadUpdate', lead);
        res.json(lead);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

app.delete('/api/leads/:phone', async (req, res) => {
    try {
        const phone = normalizePhone(req.params.phone);
        await Lead.findOneAndDelete({ phone });
        io.emit('leadDelete', { phone });
        res.json({ success: true });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// Salva vários telefones como leads de uma vez (upsert — não sobrescreve dados existentes)
app.post('/api/leads/batch', async (req, res) => {
    try {
        const { phones } = req.body;
        if (!Array.isArray(phones) || phones.length === 0)
            return res.status(400).json({ error: 'phones obrigatório' });

        const now = new Date();
        // Aceita tanto { phones: [...] } quanto { contacts: [{phone, name}] }
        const contacts = req.body.contacts
            ? req.body.contacts
            : phones.map(p => ({ phone: p, name: '' }));

        const ops = contacts.map(({ phone: rawPhone, name }) => {
            const phone = normalizePhone(String(rawPhone));
            return {
                updateOne: {
                    filter: { phone },
                    update: {
                        $set: { updatedAt: now },
                        $setOnInsert: { phone, tags: [] },
                        ...(name ? { $set: { name, updatedAt: now } } : {})
                    },
                    upsert: true
                }
            };
        });

        await Lead.bulkWrite(ops, { ordered: false });
        res.json({ success: true, saved: contacts.length });
    } catch (e) {
        console.error('Erro leads/batch:', e.message);
        res.status(500).json({ error: e.message });
    }
});

// ============================================
// ROTAS DE QUICK REPLIES
// ============================================
app.get('/api/quick-replies', async (req, res) => {
    try {
        const replies = await QuickReply.find().lean();
        res.json(replies);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

app.post('/api/quick-replies', async (req, res) => {
    try {
        const reply = await QuickReply.create(req.body);
        res.json(reply);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

app.delete('/api/quick-replies/:id', async (req, res) => {
    try {
        await QuickReply.findByIdAndDelete(req.params.id);
        res.json({ success: true });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// ============================================
// ROTAS DE CONFIG
// ============================================
app.get('/api/config', async (req, res) => {
    try {
        const config = await getConfig() || {};
        res.json(config);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// Move a tag inteira entre as listas, sem renomear/remover tags dos contatos.
app.post('/api/tags/worked', async (req, res) => {
    try {
        const { tag, worked } = req.body || {};
        if (typeof tag !== 'string' || !tag.trim() || typeof worked !== 'boolean') {
            return res.status(400).json({ error: 'Informe a tag e o destino corretamente.' });
        }
        const current = await Config.findOne().lean();
        const exists = Object.prototype.hasOwnProperty.call(current?.tagColors || {}, tag)
            || await Lead.exists({ tags: tag });
        if (!exists) return res.status(404).json({ error: 'Tag não encontrada.' });
        const config = await Config.findOneAndUpdate({}, worked
            ? { $addToSet: { workedTags: tag } }
            : { $pull: { workedTags: tag } },
            { upsert: true, new: true, lean: true });
        _configCache = config;
        _configCacheTime = Date.now();
        io.emit('tagsUpdated');
        res.json({ tag, worked, workedTags: config.workedTags || [] });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

app.post('/api/config', async (req, res) => {
    try {
        // O destino das tags só muda pelo endpoint atômico acima.
        const { workedTags, ...configFields } = req.body || {};
        const config = await Config.findOneAndUpdate({}, { $set: configFields }, { upsert: true, new: true, lean: true });
        // Atualiza o cache imediatamente ao salvar
        _configCache = config;
        _configCacheTime = Date.now();
        res.json(config);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// ============================================
// TEMPLATES META
// ============================================
app.get('/api/meta/templates', async (req, res) => {
    try {
        const config = await getConfig();
        if (!config?.metaAccessToken) return res.json([]);

        if (templateCache.data.length > 0 && (Date.now() - templateCache.lastUpdate < 300000)) {
            return res.json(templateCache.data);
        }

        const response = await axios.get(
            `https://graph.facebook.com/${META_GRAPH_API_VERSION}/${config.metaBusinessAccountId}/message_templates`,
            { headers: { Authorization: `Bearer ${config.metaAccessToken}` }, timeout: 8000 }
        );

        templateCache.data = response.data.data || [];
        templateCache.lastUpdate = Date.now();
        res.json(templateCache.data);
        
    } catch (e) {
        res.json(templateCache.data || []);
    }
});

// ============================================
// SHUTDOWN
// ============================================
process.on('SIGTERM', () => {
    server.close(() => {
        mongoose.connection.close(false, () => process.exit(0));
    });
});

// ============================================
// INICIALIZAÇÃO
// ============================================
const PORT = process.env.PORT || 8080;

server.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 Servidor na porta ${PORT}`);
});
