/// <reference path="../typedefs/node.d.ts" />
export declare class StringBuffer {
    static VERSION: string;
    data: string[];
    constructor(s: string);
    is_empty(): boolean;
    isEmpty(): boolean;
    add(s: string): void;
    add_line(s: string): void;
    addLine(s: string): void;
    newline(): void;
    newLine(): void;
    to_string(trim?: boolean): string;
    toString(trim?: boolean): string;
    as_lines(): string[];
    asLines(): string[];
    private populated(s);
}
