import {SqlAdapter} from "./sqlAdapter";

export const Sql = (Q:any) => {
    Q.adapter("sql", SqlAdapter);
};
