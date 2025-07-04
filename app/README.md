# Run and deploy your AI Studio app

This contains everything you need to run your app locally.

## Installing dependencies

**Prerequisites:** Node.js

Install project dependencies with:

```bash
npm install
```

## Running the development server

1. Set the `GEMINI_API_KEY` in [.env.local](.env.local) to your Gemini API key.
2. (Optional) Set `VITE_API_BASE` in [.env.local](.env.local) to the backend API base URL (defaults to `http://localhost:8000`).
3. Start the dev server:

```bash
npm run dev
```

## Executing tests

Run the unit tests with:

```bash
npm run test
```

## Building for production

Create a production build with:

```bash
npm run build
```

Preview the built app locally with:

```bash
npm run preview
```

## Pagination controls

The storage inspector lists WAL and MemTable entries in pages of 50 items. Use the Next and Prev buttons to navigate through results. These controls adjust the `offset` and `limit` query parameters sent to the backend.
