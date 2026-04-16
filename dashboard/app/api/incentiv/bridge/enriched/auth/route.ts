import { NextResponse } from 'next/server';

export const dynamic = 'force-dynamic';

export async function POST(request: Request) {
  try {
    const { password } = await request.json();
    const correct = process.env.BRIDGE_ENRICHED_PASSWORD;

    if (!correct) {
      // If no password is set, allow access (dev mode)
      return NextResponse.json({ valid: true });
    }

    if (!password || password !== correct) {
      return NextResponse.json({ valid: false }, { status: 401 });
    }

    return NextResponse.json({ valid: true });
  } catch {
    return NextResponse.json({ valid: false, error: 'Invalid request' }, { status: 400 });
  }
}
