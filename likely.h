#pragma once

#if __GNUC__
#define DETAIL_BUILTIN_EXPECT(b, t) (__builtin_expect(b, t))
#else
#define DETAIL_BUILTIN_EXPECT(b, t) b
#endif

#define LIKELY(x) DETAIL_BUILTIN_EXPECT((x), 1)
#define UNLIKELY(x) DETAIL_BUILTIN_EXPECT((x), 0)

#if defined(__GNUC__)
#define LIKELY(x) (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
#define LIKELY(x) (x)
#define UNLIKELY(x) (x)
#endif
