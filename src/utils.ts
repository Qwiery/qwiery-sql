import _ from 'lodash';

export default class Utils {
    static nodeSpecsToRecord(specs) {
        return {
            id: specs.id,
            labels: (specs.labels || []).join(','),
            data: specs.data,
        };
    }

    static edgeSpecsToRecord(specs) {
        return {
            id: specs.id,
            sourceId: specs.sourceId,
            targetId: specs.targetId,
            labels: (specs.labels || []).join(','),
            data: specs.data,
        };
    }

    /**
     * Turns a Qwiery edge to a Sqlite record.
     * @param e {*} An edge.
     */
    static edgeToDbRecord(e) {
        const u = _.clone(e);
        const r = {
            id: u.id,
            labels: (u.labels || []).join(','),
            sourceId: u.sourceId,
            targetId: u.targetId,
        };
        delete u.id;
        delete u.labels;
        delete u.sourceId;
        delete u.targetId;
        r.data = u;
        return r;
    }

    static nodeToDbRecord(n) {
        const u = _.clone(n);
        const r = {
            id: u.id,
            labels: (u.labels || []).join(','),
        };
        delete u.id;
        delete u.labels;
        r.data = u;
        return r;
    }

    static dbRecordToEdge(r) {
        const e = {
            id: r.id,
            sourceId: r.sourceId,
            targetId: r.targetId,
            labels: (r.labels || '').split(',').filter((u) => u.toString().trim().length > 0),
        };
        _.assign(e, r.data);
        return e;
    }

    static dbRecordToNode(r) {
        const n = {
            id: r.id,
            labels: (r.labels || '').split(',').filter((u) => u.toString().trim().length > 0),
        };
        _.assign(n, r.data);
        return n;
    }
}
