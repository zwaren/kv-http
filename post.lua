local file = assert(io.open("post.bin", 'rb'))
wrk.method = "POST"
wrk.body   = file:read(116)
wrk.headers["Content-Length"] = #wrk.body
