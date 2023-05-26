ref_alias_list = [i + 10 for i, x in enumerate([x for x in range(0, 100000)])]
reply = next((v for v, v in enumerate(ref_alias_list)), 15)
reply_default = next((v for v, v in enumerate(ref_alias_list) if 1 == 0), 15)
print(reply)
print(type(reply))
print(reply_default)

# next has a default value option which in this example is 15.
# it uses the default value if the iteration is exhausted without a return value


