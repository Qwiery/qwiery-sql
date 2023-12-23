import { describe, test, it, expect, vi, afterAll } from "vitest";
import setupSqliteSchema from "../src/sqlSchema";
import { Qwiery } from "@orbifold/dal";
import {Sql} from "../src/";
import { Utils } from "@orbifold/utils";
import AdapterUtils from "../src/utils";
import _ from "lodash";
import { SequelizeForeignKeyConstraintError, SequelizeValidationError } from "sequelize";
import path from "path";
import fs from "fs";

describe("Adapter", function () {
	afterAll(() => {
		// even though Sql is in-memory a file called "memory" is created in the root
		const what = path.join(process.cwd(), "memory");
		if (fs.existsSync(what)) {
			Utils.deleteFileOrDirectory(what);
		}
	});

	it("should allow configuration", async () => {
		const f = vi.fn();
		const p = (Q) => {
			Q.adapter("A", (opt, done) => {
				f(opt);
			});
		};
		Qwiery.plugin(p);
		expect(_.keys(Qwiery.adapterDic)).toHaveLength(2);
		let q = new Qwiery();
		expect(f).not.toHaveBeenCalled();
		let options = { adapters: ["A"], A: { x: 3 } };
		q = new Qwiery(options);
		// internally assigned if not given
		options.id = q.id;
		// an adapter init is called with all options, not just the ones for the specific adapter
		expect(f).toHaveBeenCalledWith(options);

		const filePath = path.join(__dirname, `${Utils.randomId()}.sqlite`);

		// generic way to configure plugins
		Qwiery.plugin(Sql);
		q = new Qwiery({
			adapters: ["sql"],
			sql: {
				dialect: "sqlite",
				storage: filePath,
				recreateTables: true,
			},
		});

		await q.createNode("n1");
		expect(await q.nodeExists("n1")).toBeTruthy();
		// note that the file is created AFTER the first API call and NOT when Qwiery is instantiated
		expect(fs.existsSync(filePath)).toBeTruthy();
		Utils.deleteFileOrDirectory(filePath);
	});

	it("should get all node labels", async () => {
		Qwiery.plugin(Sql);
		const q = new Qwiery({
			adapters: ["sql"],
			sql: {
				recreateTables: true,
			},
		});

		await q.createNode({ id: "a", labels: ["A", "B"] });
		await q.createNode({ id: "b", labels: ["A", "C"] });
		let labels = await q.getNodeLabels();
		expect(labels).toEqual(["A", "B", "C"]);
	});

	it("should get all edge labels", async () => {
		Qwiery.plugin(Sql);
		const q = new Qwiery({
			adapters: ["sql"],
			sql: {
				recreateTables: true,
			},
		});

		const a = await q.createNode({ id: "a" });
		const b = await q.createNode({ id: "b" });
		const e1 = await q.createEdge(a, b, null, "e1", ["A", "B"]);
		const e2 = await q.createEdge(a, b, null, "e2", ["A", "C"]);
		let labels = await q.getEdgeLabels();
		expect(labels).toEqual(["A", "B", "C"]);
	});

	it("should sequelize a graph", async function () {
		const { db, Node, Edge } = await setupSqliteSchema({ recreateTables: true });
		const a = await Node.create({ id: "a", data: { z: -1 } });
		const b = await Node.create({ id: "b" });
		const e = await Edge.create({
			id: "e",
			sourceId: "a",
			targetId: "b",
		});
		const found = await Node.findOne({ where: { id: "a" } });
		expect(found.id).toEqual("a");
		expect(found.data.z).toEqual(-1);
		expect(await Edge.count()).toEqual(1);
		expect(await Node.count()).toEqual(2);

		expect((await e.getSource()).id).toEqual("a");
		expect((await e.getTarget()).id).toEqual("b");
		expect(await Edge.count()).toEqual(1);
		expect(await Node.count()).toEqual(2);
		await Edge.destroy({
			where: {
				sourceId: "a",
			},
		});

		await Node.destroy({
			where: {
				id: "a",
			},
		});
		expect(await Edge.count()).toEqual(0);
		expect(await Node.count()).toEqual(1);
	});

	it("should use on-disk Sql", async () => {
		const filePath = path.join(__dirname, `${Utils.randomId()}.sqlite`);
		const { db, Node, Edge } = await setupSqliteSchema({
			dialect: "sqlite",
			storage: filePath,
			recreateTables: true,
		});
		expect(fs.existsSync(filePath)).toBeTruthy();

		const a = await Node.create({ id: "a", labels: "A", data: { x: -4 } });
		const b = await Node.create({ id: "b", labels: "A,B", data: { x: -5 } });
		const e = await Edge.create({ id: "e", sourceId: "a", targetId: "b", labels: "C", data: { x: 12 } });

		let found = (await Node.findOne({ where: { id: "a" } })) || null;
		expect(found).not.toBeNull();
		expect(found.data.x).toEqual(-4);
		found = (await Edge.findOne({ where: { id: "e" } })) || null;
		expect(found).not.toBeNull();
		expect(found.data.x).toEqual(12);
		Utils.deleteFileOrDirectory(filePath);
		expect(fs.existsSync(filePath)).not.toBeTruthy();
	});

	it("should not allow edges without endpoints", async () => {
		// all of this is not graph specific but foreign key behavior of any SQL store
		const { db, Node, Edge } = await setupSqliteSchema({ recreateTables: true });
		await expect(Node.create()).rejects.toThrow(SequelizeValidationError);
		await expect(Edge.create({ id: "2" })).rejects.toThrow(SequelizeValidationError);
		const a = await Node.create({ id: "a" });
		// second endpoint does not exist
		await expect(Edge.create({ id: "e", sourceId: "a", targetId: "c" })).rejects.toThrow(SequelizeForeignKeyConstraintError);
	});

	it("should switch between records and graph elements", function () {
		const n = {
			id: "a",
			labels: ["a", "b"],
			x: -1,
			y: 2,
		};
		let r = AdapterUtils.nodeToDbRecord(n);
		expect(r.data).toEqual({ x: -1, y: 2 });
		const n2 = AdapterUtils.dbRecordToNode(r);
		expect(n2).toEqual(n);

		const e = {
			id: "e",
			sourceId: "a",
			targetId: "b",
			labels: ["a", "b"],
			x: -4,
			y: "r",
		};
		r = AdapterUtils.edgeToDbRecord(e);
		expect(r.data).toEqual({ x: -4, y: "r" });
		const e2 = AdapterUtils.dbRecordToEdge(r);
		expect(e2).toEqual(e);
	});

	it("should set a default node label", async function () {
		Qwiery.plugin(Sql);
		// uses default connection info to Neo4j
		const q = new Qwiery({
			adapters: ["sql"],
			sql: {
				recreateTables: true,
			},
		});
		let node = await q.createNode({ name: "ump", u: 45, m: 7, p: -2 });
		expect(await q.nodeExists(node.id)).toBeTruthy();
		node = await q.getNode(node.id);
		expect(node.name).toEqual("ump");
		// in a semantic spirit, the default label is a Thing
		expect(node.labels).toEqual(["Thing"]);
	});

	it("should throw if node id exists", async function () {
		Qwiery.plugin(Sql);
		const q = new Qwiery({
			adapters: ["sql"],
			sql: {
				recreateTables: true,
			},
		});
		let a = await q.createNode(Utils.id());
		await expect(q.createNode(a.id)).rejects.toThrow(Error);
	});

	it("should throw if edge id exists", async function () {
		Qwiery.plugin(Sql);
		const q = new Qwiery({
			adapters: ["sql"],
			sql: {
				recreateTables: true,
			},
		});
		let a = await q.createNode(Utils.id());
		let b = await q.createNode(Utils.id());
		const edgeId = Utils.id();
		await q.createEdge(a.id, b.id, null, edgeId);
		await expect(q.createEdge(a.id, b.id, null, edgeId)).rejects.toThrow(Error);
	});

	it("should crud edges", async function () {
		Qwiery.plugin(Sql);
		const q = new Qwiery({
			adapters: ["sql"],
			sql: {
				recreateTables: true,
			},
		});
		let a = await q.createNode(Utils.id());
		let b = await q.createNode(Utils.id());
		const e1 = Utils.id();
		const e2 = Utils.id();
		await q.createEdge(a.id, b.id, null, e1);
		await q.createEdge({
			sourceId: a.id,
			targetId: b.id,
			id: e2,
			x: -4,
			y: 23,
		});
		let found = await q.getEdgeBetween(a.id, b.id);
		expect(found.id).toEqual(e1);
		expect(await q.edgeExists(e1)).toBeTruthy();
		expect(await q.edgeExists(e2)).toBeTruthy();
		let ed = await q.getEdge(e2);
		expect(ed.x).toEqual(-4);
		expect(ed.y).toEqual(23);

		await q.updateEdge({
			sourceId: a.id,
			targetId: b.id,
			id: e2,
			x: 4,
			y: 23,
		});
		ed = await q.getEdge(e2);
		expect(ed.x).toEqual(4);
		expect(ed.y).toEqual(23);

		await q.deleteEdge({ id: e2 });
		expect(await q.edgeExists(e2)).toBeFalsy();
		let e3 = Utils.id();
		const label = Utils.randomLetters(3);
		await q.createEdge({
			sourceId: a.id,
			targetId: b.id,
			labels: [label],
			id: e3,
		});
		let edl = await q.getEdgeWithLabel(a.id, b.id, label);
		expect(edl.id).toEqual(e3);
		expect(await q.getEdgesWithLabel(label)).toHaveLength(1);
	});

	it("should update a node", async function () {
		Qwiery.plugin(Sql);
		const q = new Qwiery({
			adapters: ["sql"],
			sql: {
				recreateTables: true,
			},
		});
		let a = await q.createNode({
			name: "a",
			x: 9,
		});
		expect(await q.nodeExists(a.id)).toBeTruthy();
		await q.updateNode({
			id: a.id,
			name: "b",
		});
		const found = await q.getNode(a.id);
		expect(found.name).toEqual("b");
		expect(found.x).toBeUndefined();
	});

	it("should delete an edge", async function () {
		Qwiery.plugin(Sql);
		const q = new Qwiery({
			adapters: ["sql"],
			sql: {
				recreateTables: true,
			},
		});
		let a = await q.createNode(Utils.id());
		let b = await q.createNode(Utils.id());
		const e1 = Utils.id();
		await q.createEdge(a.id, b.id, null, e1);
		expect(await q.edgeExists(e1)).toBeTruthy();

		await q.deleteEdge(e1);
		expect(await q.edgeExists(e1)).toBeFalsy();
	});

	it("should get nodes with a specific label", async function () {
		Qwiery.plugin(Sql);
		const q = new Qwiery({
			adapters: ["sql"],
		});
		const A = "A" + Utils.randomInteger(1, 100000);
		const B = "B" + Utils.randomInteger(1, 100000);
		const C = "C" + Utils.randomInteger(1, 100000);
		console.log(A, B, C);
		const a = await q.createNode({
			labels: ["A", "B"],
			name: "a",
		});
		const b = await q.createNode({
			labels: ["A", "C"],
			name: "b",
		});
		const as = await q.getNodesWithLabel("A");
		const bs = await q.getNodesWithLabel("B");
		const cs = await q.getNodesWithLabel("C");
		expect(as).toHaveLength(2);
		expect(bs).toHaveLength(1);
		expect(cs).toHaveLength(1);
	});

	it("should upsert elements", async function () {
		Qwiery.plugin(Sql);
		const q = new Qwiery({
			adapters: ["sql"],
			sql: {
				recreateTables: true,
			},
		});
		let a = await q.createNode(Utils.id());
		let b = await q.createNode(Utils.id());
		const e1 = Utils.id();
		await q.upsertEdge({
			sourceId: a.id,
			targetId: b.id,
			id: e1,
		});
		expect(await q.edgeExists(e1)).toBeTruthy();

		await q.deleteEdge(e1);
		expect(await q.edgeExists(e1)).toBeFalsy();

		await q.upsertEdge({
			sourceId: a.id,
			targetId: b.id,
			id: e1,
			x: 55,
		});
		const e = await q.getEdge(e1);
		expect(e.x).toEqual(55);

		// upsert a
		await q.upsertNode({
			id: a.id,
			h: 0.9,
		});
		a = await q.getNode(a.id);
		expect(a.h).toEqual(0.9);

		await q.upsertNode({
			id: a.id,
			h: 0.9,
		});

		let r = await q.upsertNode({
			id: Utils.id(),
			s: "A",
		});
		r = await q.getNode(r.id);
		expect(r.s).toEqual("A");
		await q.deleteNode(r.id);
		expect(await q.nodeExists(r.id)).toBeFalsy();
	});

	it("should use Mongo projections", async function () {
		Qwiery.plugin(Sql);
		const q = new Qwiery({
			adapters: ["sql"],
			sql: {
				recreateTables: true,
			},
		});
		const prefix = Utils.randomLetters(3);
		const a = await q.createNode({ id: "a", x: 9, y: 45 });
		console.log(">>>", JSON.stringify(a));

		let found = await q.getNode({ x: 9 });
		expect(found).not.toBeNull();
		expect(found.y).toEqual(45);

		found = await q.getNode({ x: { $gt: 8 } });
		expect(found).not.toBeNull();
		expect(found.y).toEqual(45);

		found = await q.getNode({ $and: [{ x: { $gt: 8 } }, { x: { $lte: 9 } }] });
		expect(found).not.toBeNull();
		expect(found.y).toEqual(45);

		await q.createNodes([{ name: `${prefix}1` }, { name: `${prefix}2` }, { name: `${prefix}3` }, { name: `${prefix}4` }]);
		found = await q.getNodes({ name: { $startsWith: prefix } });
		expect(found).toHaveLength(4);

		await q.createNodes([{ name: `abc${prefix}efg` }]);
		found = await q.getNodes({ name: { $contains: prefix } });
		expect(found.length).toBeGreaterThan(0);

		// ensure repeatable tests
		let d = {};
		d[prefix] = [4, 5, 6, 7, 88];
		let n = await q.createNode(d); //?
		let pred = {};
		pred[prefix] = { $size: 5 };
		found = await q.getNodes(pred); //?
		expect(found).toHaveLength(1);
		expect(found[0].id).toEqual(n.id);
		pred = {};
		pred[prefix] = { $all: [88] };
		// found = await q.getNodes(pred);//?
		// expect(found[0].id).toEqual(n.id);
	});

	it("should count nodes", async function () {
		Qwiery.plugin(Sql);
		const q = new Qwiery({
			adapters: ["sql"],
			sql: {
				recreateTables: true,
			},
		});
		const prefix = Utils.randomLetters(3);
		expect(await q.nodeCount()).toEqual(0);
		await q.createNodes(_.range(10).map((i) => ({ name: `${prefix}${i}` })));
		let count = await q.nodeCount({ name: { $startsWith: prefix } });
		expect(count).toEqual(10);
	});

	it("should count edges", async function () {
		Qwiery.plugin(Sql);
		const q = new Qwiery({
			adapters: ["sql"],
		});
		const prefix = Utils.randomLetters(3);
		let a = await q.createNode(Utils.id());
		let b = await q.createNode(Utils.id());
		let edges = _.range(10).map((i) => ({
			sourceId: a.id,
			targetId: b.id,
			name: `${prefix}${i}`,
			id: Utils.id(),
		}));
		for (const edge of edges) {
			await q.createEdge(edge);
		}
		let count = await q.edgeCount({ name: { $startsWith: prefix } });
		expect(count).toEqual(10);
		let found = await q.getEdge({ id: edges[0].id });
		expect(found.name).toEqual(`${prefix}0`);

		found = await q.getEdges({ id: edges[0].id });
		expect(found).toHaveLength(1);
	});

	it("should delete nodes", async function () {
		Qwiery.plugin(Sql);
		const q = new Qwiery({
			adapters: ["sql"],
			sql: {
				recreateTables: true,
			},
		});

		await q.createNodes(_.range(100).map((i) => ({ id: i, n: i })));

		let count = await q.nodeCount();
		expect(count).toEqual(100);
		// await q.deleteNodes({n: {$gt: 50}});
		let found = await q.getNodes({ n: { $gt: 1 } });
		// expect(count).toEqual(50);
		expect(found).toHaveLength(98);
		await q.deleteNodes({ n: { $gt: 1 } });
		count = await q.nodeCount();
		expect(count).toEqual(2);
	});

	it("should clear the graph", async function () {
		Qwiery.plugin(Sql);
		const q = new Qwiery({
			adapters: ["sql"],
			sql: {
				recreateTables: true,
			},
		});
		expect(await q.nodeCount()).toEqual(0);
		await q.createNodes(_.range(100).map((i) => ({ id: i, n: i })));
		let nodeCount = await q.nodeCount();
		expect(nodeCount).toEqual(100);
		await q.createEdge("0", "1");
		let edgeCount = await q.edgeCount();
		expect(edgeCount).toEqual(1);
		await q.clear();
		nodeCount = await q.nodeCount();
		expect(nodeCount).toEqual(0);
		edgeCount = await q.edgeCount();
		expect(edgeCount).toEqual(0);
	});

	it("should give you Sql power", async function () {
		Qwiery.plugin(Sql);
		const q = new Qwiery({
			adapters: ["sql"],
			sql: {
				recreateTables: true,
			},
		});
		let a = await q.createNode(Utils.id());
		let b = await q.createNode(Utils.id());
		const e1 = Utils.id();
		const e2 = Utils.id();
		await q.createEdge(a.id, b.id, null, e1);
		await q.createEdge({
			sourceId: a.id,
			targetId: b.id,
			id: e2,
			x: -4,
			y: 23,
		});
		let found = await q.getEdgeBetween(a.id, b.id);
		expect(found.id).toEqual(e1);
		expect(await q.edgeExists(e1)).toBeTruthy();
		expect(await q.edgeExists(e2)).toBeTruthy();
		let ed = await q.getEdge(e2);
		expect(ed.x).toEqual(-4);
		expect(ed.y).toEqual(23);

		await q.updateEdge({
			sourceId: a.id,
			targetId: b.id,
			id: e2,
			x: 4,
			y: 23,
		});
		ed = await q.getEdge(e2);
		expect(ed.x).toEqual(4);
		expect(ed.y).toEqual(23);

		await q.deleteEdge({ id: e2 });
		expect(await q.edgeExists(e2)).toBeFalsy();
		let e3 = Utils.id();
		const label = Utils.randomLetters(3);
		await q.createEdge({
			sourceId: a.id,
			targetId: b.id,
			labels: [label],
			id: e3,
		});
		let edl = await q.getEdgeWithLabel(a.id, b.id, label);
		expect(edl.id).toEqual(e3);
		expect(await q.getEdgesWithLabel(label)).toHaveLength(1);
	});

	it("should infer the schema", async () => {
		Qwiery.plugin(Sql);
		const q = new Qwiery({
			adapters: ["sql"],
			sql: {
				recreateTables: true,
			},
		});

		let a = await q.createNode({ labels: ["A"] });
		let b = await q.createNode({ labels: ["B"] });
		let c = await q.createNode({ labels: ["B"] });
		let e1 = await q.createEdge(a, b, null, "e1", ["E1"]);

		let schema = await q.inferSchemaGraph();
		expect(schema.nodeCount).toEqual(2);
		expect(schema.edgeCount).toEqual(1);

		let e2 = await q.createEdge(a, b, null, "e2", ["E2"]);
		schema = await q.inferSchemaGraph();
		expect(schema.nodeCount).toEqual(2);
		expect(schema.edgeCount).toEqual(2);

		await q.updateNode({
			id: a.id,
			labels: ["A", "C"],
		});
		schema = await q.inferSchemaGraph();
		expect(schema.nodeCount).toEqual(3);
		expect(schema.edgeCount).toEqual(4);
	});

	it("should path-query", async () => {
		Qwiery.plugin(Sql);
		const g = new Qwiery({
			adapters: ["sql"],
			sql: {
				recreateTables: true,
			},
		});
		let a = await g.createNode({ id: "a", labels: ["A"] });
		let b = await g.createNode({ id: "b", labels: ["B"] });
		let c = await g.createNode({ id: "c", labels: ["C"] });

		let e1 = await g.createEdge("a", "b", null, "e1", "E1");
		let e2 = await g.createEdge("b", "c", null, "e2", "E2");

		let found = await g.pathQuery(["A", "*", "B"]);
		expect(found.nodeCount).toEqual(2);
		expect(found.edgeCount).toEqual(1);
		expect(found.edges[0].labels).toEqual(["E1"]);

		found = await g.pathQuery(["A", "*", "B", "*", "C"]);
		// console.log(JSON.stringify(found.toJSON(), null, 2));
		expect(found.nodeCount).toEqual(3);
		expect(found.edgeCount).toEqual(2);
		expect(found.edges[0].labels).toEqual(["E1"]);
		expect(found.edges[1].labels).toEqual(["E2"]);

		// can specify both an edge and a node label
		await expect(g.pathQuery([""])).rejects.toThrow(Error);
		await expect(g.pathQuery(["A", ""])).rejects.toThrow(Error);

		found = await g.pathQuery(["C"]);
		expect(found.nodeCount).toEqual(1);
		expect(found.edgeCount).toEqual(0);

		found = await g.pathQuery(["*", "E1", "*"]);
		expect(found.nodeCount).toEqual(2);
		expect(found.edgeCount).toEqual(1);

		found = await g.pathQuery(["*", "E1", "*", "E2", "*"]);
		expect(found.nodeCount).toEqual(3);
		expect(found.edgeCount).toEqual(2);
	}, 30000);

	it("should get nodes and respect the amount", async () => {
		Qwiery.plugin(Sql);
		const g = new Qwiery({
			adapters: ["sql"],
			sql: {
				recreateTables: true,
			},
		});

		const N = Utils.randomInteger(100, 1000);
		for (const i of _.range(N)) {
			await g.createNode(i);
		}
		expect(await g.nodeCount()).toEqual(N);
		let ns = await g.getNodes();
		expect(ns.length).toEqual(N);
		const count = Utils.randomInteger(3, 100);
		ns = await g.getNodes({}, count);
		expect(ns.length).toEqual(count);
	});

	it("should search for nodes", async () => {
		Qwiery.plugin(Sql);
		const g = new Qwiery({
			adapters: ["sql"],
			sql: {
				recreateTables: true,
			},
		});
		for (let i = 0; i < 300; i++) {
			await g.createNode({
				id: "A" + i,
				labels: ["A"],
				name: "A " + i,
			});
		}
		for (let i = 0; i < 150; i++) {
			await g.createNode({
				id: "B" + i,
				labels: ["B"],
				name: "B " + i,
			});
		}
		await g.createNode({
			id: "C1",
			labels: ["Cell"],
			name: "a",
		});
		await g.createNode({
			id: "C2",
			labels: ["Cell"],
			name: "b",
		});
		let found = await g.searchNodes("A");
		// default amount is 100
		expect(found).toHaveLength(100);

		found = await g.searchNodes("c", ["labels"]);
		expect(found).toHaveLength(2);

		found = await g.searchNodesWithLabel("b", ["name"], "Cell");
		expect(found).toHaveLength(1);
		expect(found[0].id).toEqual("C2");
	});

	it("should get the neighborhood graph", async () => {
		Qwiery.plugin(Sql);
		const g = new Qwiery({
			adapters: ["sql"],
			sql: {
				recreateTables: true,
			},
		});
		const root = await g.createNode("root");
		for (let i = 0; i < 30; i++) {
			const n = await g.createNode({
				id: "A" + i,
				labels: ["A"],
				name: "A " + i,
			});
			await g.createEdge(root.id, n.id);
		}
		const ng = await g.getNeighborhood(root.id, 1000);
		expect(ng.nodeCount).toEqual(31);
		expect(ng.edgeCount).toEqual(30);
	});

	it("should get node label properties", async () => {
		Qwiery.plugin(Sql);
		const g = new Qwiery({
			adapters: ["sql"],
			sql: {
				recreateTables: true,
			},
		});
		await g.createNode({
			labels: ["A"],
			x: 2,
		});
		await g.createNode({
			labels: ["A"],
			y: 2,
		});
		await g.createNode({
			labels: ["A"],
			y: 2,
			z: 5,
		});
		let found = await g.getNodeLabelProperties("A");
		found.sort();
		// the id is always added even if not given
		expect(found).toEqual(["id", "x", "y", "z"]);
	});
});
