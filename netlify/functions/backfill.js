
// netlify/functions/backfill.js

exports.handler = async function(event) {
  // CORS preflight handling
  if (event.httpMethod === 'OPTIONS') {
    return {
      statusCode: 204,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST,OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
      },
    };
  }

  if (event.httpMethod !== 'POST') {
    return {
      statusCode: 405,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: 'Method Not Allowed',
    };
  }

  let payload;
  try {
    payload = JSON.parse(event.body);
  } catch {
    return {
      statusCode: 400,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: 'Bad JSON',
    };
  }

  const { start_date, end_date } = payload;
  if (!start_date || !end_date) {
    return {
      statusCode: 400,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: 'Missing dates',
    };
  }

  const token    = process.env.GITHUB_DISPATCH_TOKEN;
  const repo     = 'finncampbell/fund-tracker';
  const workflow = 'fund-tracker.yml';

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
    return {
      statusCode: resp.status,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: `GitHub API error: ${errorText}`
    };
  }

  return {
    statusCode: 200,
    headers: { 'Access-Control-Allow-Origin': '*' },
    body: JSON.stringify({
      message: `Fund Tracker run dispatched for ${start_date} → ${end_date}`
    })
  };
};
