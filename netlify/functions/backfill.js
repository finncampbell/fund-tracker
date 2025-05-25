exports.handler = async function(event) {
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, body: JSON.stringify({ error: 'Method Not Allowed' }) };
  }

  let payload;
  try {
    payload = JSON.parse(event.body);
  } catch {
    return { statusCode: 400, body: JSON.stringify({ error: 'Bad JSON' }) };
  }

  const { start_date, end_date } = payload;
  if (!start_date || !end_date) {
    return { statusCode: 400, body: JSON.stringify({ error: 'Missing dates' }) };
  }

  const token    = process.env.GITHUB_DISPATCH_TOKEN;
  const repo     = 'finncampbell/fund-tracker';
  const workflow = 'backfill-week.yml';

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
    return { statusCode: resp.status, body: JSON.stringify({ error: errorText }) };
  }

  return {
    statusCode: 200,
    body: JSON.stringify({ message: `Backfill dispatched for ${start_date} → ${end_date}` })
  };
};
