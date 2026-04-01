/// <reference types="vite/client" />

declare module "virtual:dbt-lineage" {
  const lineage: Record<
    string,
    { refs: string[]; sources: string[] }
  >;
  export default lineage;
}
