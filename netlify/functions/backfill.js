// netlify/functions/backfill.js

export async function handler(event) {
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, body: 'Method Not Allowed' };
  }

  let payload;
  try {
    payload = JSON.parse(event.body);
  } catch {
    return { statusCode: 400, body: 'Bad JSON' };
  }
  const { start_date, end_date } = payload;
  if (!start_date || !end_date) {
    return { statusCode: 400, body: 'Missing dates' };
  }

  const token    = process.env.GITHUB_DISPATCH_TOKEN;
  const repo     = 'finncampbell/fund-tracker';
  const workflow = 'backfill-week.yml';

  // Use native fetch
  const resp = await fetch(
    `https://api.github.com/repos/${repo}/actions/workflows/${workflow}/dispatches`,
    {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${token}`,
        Accept:        'application/vnd.github.v3+json',
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        ref: 'main',
        inputs: { start_date, end_date }
      })
    }
  );

  if (!resp.ok) {
    const errorText = await resp.text();
    return { statusCode: resp.status, body: `GitHub API error: ${errorText}` };
  }

  return {
    statusCode: 200,
    body: JSON.stringify({ message: `Backfill dispatched for ${start_date} → ${end_date}` })
  };
}
