/**
 * Example: Next.js API route using Vercel AI SDK with OrionBelt tools.
 *
 * Place this file at: app/api/chat/route.ts
 *
 * Prerequisites:
 *   npm install ai @ai-sdk/anthropic zod
 *
 * Environment variables:
 *   ANTHROPIC_API_KEY=sk-ant-...
 *   ORIONBELT_API_URL=http://localhost:8000
 */

import { streamText } from "ai";
import { anthropic } from "@ai-sdk/anthropic";
import { getOrionBeltTools } from "./orionbelt-tools";

const SYSTEM_PROMPT = `You are a data analyst assistant powered by the OrionBelt Semantic Layer.
You help users explore semantic data models and compile analytical SQL queries.

Workflow:
1. Start by calling describeModel to understand the available data.
2. Use listDimensions, listMeasures, or listMetrics for details.
3. Use searchModel to find artefacts by name or synonym when unsure.
4. Use explainArtefact to trace lineage back to physical tables.
5. Use compileQuery to generate SQL with exact dimension and measure names.

Rules:
- Use exact artefact names from the model (case-sensitive).
- Default to postgres dialect unless the user specifies otherwise.
- Present compiled SQL in a code block with the dialect name.`;

export async function POST(req: Request) {
  const { messages } = await req.json();

  const apiUrl = process.env.ORIONBELT_API_URL || "http://localhost:8000";
  const tools = getOrionBeltTools(apiUrl);

  const result = streamText({
    model: anthropic("claude-sonnet-4-5"),
    system: SYSTEM_PROMPT,
    messages,
    tools,
    maxSteps: 10,
  });

  return result.toDataStreamResponse();
}
