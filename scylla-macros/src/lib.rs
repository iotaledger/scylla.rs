// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use proc_macro::TokenStream;
use quote::quote;
use scylla_parse::{Statement, StatementStream};

mod to_tokens;
use to_tokens::*;

#[proc_macro_derive(ColumnEncoder, attributes(column, encode, decode))]
pub fn column_encoder_derive(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    let syn::DeriveInput {
        attrs: _,
        vis: _,
        ident,
        generics,
        data,
    } = input;
    let (imp, ty, wher) = generics.split_for_impl();
    let res = match data {
        syn::Data::Struct(s) => {
            let mut call = None;
            if s.fields.len() == 0 {
                panic!("#[derive(ColumnEncoder)] can only be used on structs with fields");
            }
            for (i, f) in s.fields.iter().enumerate() {
                let encode_attr = f.attrs.iter().find(|a| a.path.is_ident("encode"));
                let col_attr = f
                    .attrs
                    .iter()
                    .find(|a| a.path.is_ident("column") || a.path.is_ident("decode"))
                    .is_some();
                if s.fields.len() == 1 || encode_attr.is_some() || col_attr {
                    if call.is_some() {
                        panic!("Only one field can have the `#[column]` or `#[encode]` attribute");
                    } else {
                        let id = f.ident.as_ref().map(|f| quote! {self.#f}).unwrap_or_else(|| {
                            let i = syn::Index::from(i);
                            quote! {self.#i}
                        });
                        if !encode_attr.map(|a| a.tokens.is_empty()).unwrap_or(true) {
                            match encode_attr.unwrap().parse_args::<syn::Type>().unwrap() {
                                syn::Type::Path(p) => {
                                    call = Some(quote! {#p(#id).encode(buffer);});
                                }
                                _ => panic!("Invalid `#[column]` argument! Must use a column type!"),
                            }
                        } else {
                            call = Some(quote! {#id.encode(buffer);});
                        }
                    }
                }
            }
            if call.is_none() {
                panic!("For structs with multiple fields, mark the wrapped column with the `#[column]` attribute");
            }

            quote! {
                impl #imp ColumnEncoder for #ident #ty #wher {
                    fn encode(&self, buffer: &mut Vec<u8>) {
                        #call
                    }
                }

                impl #imp TokenEncoder for #ident #ty #wher {
                    fn encode_token(&self) -> TokenEncodeChain {
                        self.into()
                    }
                }
            }
        }
        syn::Data::Enum(e) => {
            let variants = e
                .variants
                .iter().enumerate()
                .map(|(i, v)| {
                    let i = i as u8;
                    let syn::Variant {
                        attrs: _,
                        ident: var_ident,
                        fields,
                        discriminant: _,
                    } = v;
                    let mut call = None;
                    let mut field_ids = Vec::new();
                    if fields.is_empty() {
                        panic!("#[derive(ColumnEncoder)] can only be used on enums with variants containing at least one field");
                    }
                    
                    for f in fields.iter() {
                        let encode_attr = f.attrs.iter().find(|a| a.path.is_ident("encode"));
                        let col_attr = f.attrs.iter().find(|a| a.path.is_ident("column") || a.path.is_ident("decode")).is_some();
                        if fields.len() == 1 || encode_attr.is_some() || col_attr {
                            if call.is_some() {
                                panic!("Only one field can have the `#[column]` or `#[encode]` attribute");
                            } else {
                                let id = f.ident
                                    .as_ref()
                                    .map(|f| quote! {#f})
                                    .unwrap_or_else(|| {
                                        quote! {col}
                                    });
                                if !encode_attr.map(|a| a.tokens.is_empty()).unwrap_or(true) {
                                    match encode_attr.unwrap().parse_args::<syn::Type>().unwrap() {
                                        syn::Type::Path(p) => {
                                            call = Some(quote! {
                                                    let start = buffer.len();
                                                    buffer.extend(&[0,0,0,0]);
                                                    buffer.push(#i);
                                                    #p(#id).encode(buffer);
                                                    let len = i32::to_be_bytes(buffer.len() as i32 - start as i32 - 4);
                                                    buffer[start..start+4].copy_from_slice(&len);
                                                }
                                            );
                                        },
                                        _ => panic!("Invalid `#[encode]` argument! Must use a column type!"),
                                    }
                                } else {
                                    call = Some(quote! {
                                            let start = buffer.len();
                                            buffer.extend(&[0,0,0,0]);
                                            buffer.push(#i);
                                            #id.encode(buffer);
                                            let len = i32::to_be_bytes(buffer.len() as i32 - start as i32 - 4);
                                            buffer[start..start+4].copy_from_slice(&len);
                                        }
                                    );
                                }
                                field_ids.push(f.ident.as_ref()
                                    .map(|f| quote! {#f})
                                    .unwrap_or(quote! {col}));
                            }
                        } else {
                            field_ids.push(f.ident.as_ref()
                                .map(|f| quote! {#f: _})
                                .unwrap_or(quote! {_}));
                        }
                    }
                        
                    if call.is_none() {
                        panic!("For enum variants with multiple fields, mark the wrapped column with the `#[column]` or `#[encode]` attribute");
                    }

                    match fields {
                        syn::Fields::Named(_) => quote! { #ident::#var_ident {#(#field_ids),*} => {#call} },
                        syn::Fields::Unnamed(_) => quote! { #ident::#var_ident (#(#field_ids),*) => {#call} },
                        syn::Fields::Unit => panic!(),
                    }
                })
                ;
            quote! {
                impl #imp ColumnEncoder for #ident #ty #wher {
                    fn encode(&self, buffer: &mut Vec<u8>) {
                        match self {
                            #(#variants)*
                        }
                    }
                }

                impl #imp TokenEncoder for #ident #ty #wher {
                    fn encode_token(&self) -> TokenEncodeChain {
                        self.into()
                    }
                }
            }
        }
        syn::Data::Union(_) => panic!("Unions not supported!"),
    };
    res.into()
}

#[proc_macro_derive(ColumnDecoder, attributes(column, encode, decode))]
pub fn column_decoder_derive(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    let syn::DeriveInput {
        attrs: _,
        vis: _,
        ident,
        generics,
        data,
    } = input;
    let (imp, ty, wher) = generics.split_for_impl();
    let res = match data {
        syn::Data::Struct(s) => {
            if s.fields.is_empty() {
                panic!("#[derive(ColumnDecoder)] can only be used on structs with at least one field");
            }
            let mut marked = false;
            let calls = s
                .fields
                .iter()
                .map(|f| {
                    let decode_attr = f.attrs.iter().find(|a| a.path.is_ident("decode"));
                    let col_attr = f
                        .attrs
                        .iter()
                        .find(|a| a.path.is_ident("column") || a.path.is_ident("encode"))
                        .is_some();
                    let id = f.ident.as_ref().map(|f| quote! {#f:});
                    if s.fields.len() == 1 || decode_attr.is_some() || col_attr {
                        if marked {
                            panic!("Only one field can have the `#[column]` or `#[decode]` attribute");
                        } else {
                            marked = true;
                            if !decode_attr.map(|a| a.tokens.is_empty()).unwrap_or(true) {
                                match decode_attr.unwrap().parse_args::<syn::Type>().unwrap() {
                                    syn::Type::Path(p) => {
                                        quote! {#id #p::try_decode_column(slice)?.into()}
                                    }
                                    _ => panic!("Invalid `#[decode]` argument! Must use a column type!"),
                                }
                            } else {
                                quote! {#id ColumnDecoder::try_decode_column(slice)?}
                            }
                        }
                    } else {
                        quote! {#id Default::default()}
                    }
                })
                .collect::<Vec<_>>();

            if !marked {
                panic!("For structs with multiple fields, mark the wrapped column with the `#[column]` or `#[decode]` attribute");
            }

            let s = match s.fields {
                syn::Fields::Named(_) => quote! { Self {#(#calls),*} },
                syn::Fields::Unnamed(_) => quote! { Self (#(#calls),*) },
                syn::Fields::Unit => panic!(),
            };

            quote! {
                impl #imp ColumnDecoder for #ident #ty #wher {
                    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
                        Ok(#s)
                    }
                }
            }
        }
        syn::Data::Enum(e) => {
            let variants = e
                .variants
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    let i = i as u8;
                    let syn::Variant {
                        attrs: _,
                        ident: var_ident,
                        fields,
                        discriminant: _,
                    } = v;
                    let mut marked = false;
                    let calls = fields
                        .iter()
                        .map(|f| {
                            let decode_attr = f.attrs.iter().find(|a| a.path.is_ident("decode"));
                            let col_attr = f.attrs.iter().find(|a| a.path.is_ident("column") || a.path.is_ident("encode")).is_some();
                            let id = f.ident
                                .as_ref()
                                .map(|f| quote! {#f:});
                            if fields.len() == 1 || decode_attr.is_some() || col_attr {
                                if marked {
                                    panic!("Only one field can have the `#[column]` or `#[decode]` attribute");
                                } else {
                                    marked = true;
                                    if !decode_attr.map(|a| a.tokens.is_empty()).unwrap_or(true) {
                                        match decode_attr.unwrap().parse_args::<syn::Type>().unwrap() {
                                            syn::Type::Path(p) => {
                                                quote! {#id #p::try_decode_column(&slice[1..])?.into()}
                                            },
                                            _ => panic!("Invalid `#[decode]` argument! Must use a column type!"),
                                        }
                                    } else {
                                        quote! {#id ColumnDecoder::try_decode_column(&slice[1..])?}
                                    }
                                }
                            } else {
                                quote! {#id Default::default()}
                            }
                        }).collect::<Vec<_>>();

                    if !marked {
                        panic!(
                            "For enums with multiple fields, mark the wrapped column with the `#[column]` or `#[decode]` attribute"
                        );
                    }

                    match fields {
                        syn::Fields::Named(_) => quote! { #i => #ident::#var_ident {#(#calls),*}, },
                        syn::Fields::Unnamed(_) => quote! { #i => #ident::#var_ident (#(#calls),*), },
                        syn::Fields::Unit => panic!(),
                    }
                });
            quote! {
                impl #imp ColumnDecoder for #ident #ty #wher {
                    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
                        Ok(match slice[0] {
                            #(#variants)*
                            _ => anyhow::bail!("Invalid variant!"),
                        })
                    }
                }
            }
        }
        syn::Data::Union(_) => panic!("Unions not supported!"),
    };
    res.into()
}

#[proc_macro_derive(Column, attributes(column, encode, decode))]
pub fn column_derive(input: TokenStream) -> TokenStream {
    column_encoder_derive(input.clone())
        .into_iter()
        .chain(column_decoder_derive(input))
        .collect()
}

#[proc_macro_derive(TokenEncoder)]
pub fn token_encoder_derive(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    let syn::DeriveInput {
        attrs: _,
        vis: _,
        ident,
        generics,
        data,
    } = input;
    let (imp, ty, wher) = generics.split_for_impl();

    let res = match data {
        syn::Data::Struct(s) => {
            if s.fields.len() == 0 {
                panic!("#[derive(TokenEncoder)] can only be used on structs with fields");
            }
            let calls = s.fields.iter().enumerate().map(|(i, f)| {
                let id = f.ident.as_ref().map(|f| quote! {self.#f}).unwrap_or_else(|| {
                    let i = syn::Index::from(i);
                    quote! {self.#i}
                });
                if i == 0 {
                    quote! {TokenEncodeChain::from(&#id)}
                } else {
                    quote! {.chain(&#id)}
                }
            });

            quote! {
                impl #imp TokenEncoder for #ident #ty #wher {
                    fn encode_token(&self) -> TokenEncodeChain {
                        #(#calls)*
                    }
                }
            }
        }
        syn::Data::Enum(e) => {
            let variants = e
                .variants
                .iter()
                .map(|v| {
                    let syn::Variant {
                        attrs: _,
                        ident: var_ident,
                        fields,
                        discriminant: _,
                    } = v;
                    let mut field_ids = Vec::new();
                    if fields.is_empty() {
                        panic!("#[derive(TokenEncoder)] can only be used on enums with variants containing at least one field");
                    }
                    
                    let calls = fields.iter().enumerate().map(|(i,f)| {
                        let id = f.ident.as_ref().map(|f| quote! {#f}).unwrap_or_else(|| {
                            let i = syn::Index::from(i);
                            let id = quote::format_ident!("col{}", i);
                            quote! {#id}
                        });
                        field_ids.push(quote!(#id));
                        if i == 0 {
                            quote! {TokenEncodeChain::from(&#id)}
                        } else {
                            quote! {.chain(&#id)}
                        }
                    }).collect::<Vec<_>>();

                    match fields {
                        syn::Fields::Named(_) => quote! { #ident::#var_ident {#(#field_ids),*} => {#(#calls)*} },
                        syn::Fields::Unnamed(_) => quote! { #ident::#var_ident (#(#field_ids),*) => {#(#calls)*} },
                        syn::Fields::Unit => panic!(),
                    }
                });
            quote! {
                impl #imp TokenEncoder for #ident #ty #wher {
                    fn encode_token(&self) -> TokenEncodeChain {
                        match self {
                            #(#variants)*
                        }
                    }
                }
            }
        }
        syn::Data::Union(_) => panic!("Unions not supported!"),
    };
    res.into()
}

#[proc_macro_derive(Row)]
pub fn row_derive(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    let syn::DeriveInput {
        attrs: _,
        vis: _,
        ident,
        generics,
        data,
    } = input;
    let (imp, ty, wher) = generics.split_for_impl();
    let res = match data {
        syn::Data::Struct(s) => {
            let calls = s
                .fields
                .iter()
                .map(|f| {
                    let id = f.ident.as_ref().map(|f| quote! {#f:});
                    quote! {#id rows.column_value()?}
                });

            let s = match s.fields {
                syn::Fields::Named(_) => quote! { Self {#(#calls),*} },
                syn::Fields::Unnamed(_) => quote! { Self (#(#calls),*) },
                syn::Fields::Unit => panic!(),
            };

            quote! {
                impl #imp Row for #ident #ty #wher {
                    fn try_decode_row<R: ColumnValue>(rows: &mut R) -> anyhow::Result<Self> {
                        Ok(#s)
                    }
                }
            }
        }
        syn::Data::Enum(e) => {
            let variants = e
                .variants
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    let i = i as u8;
                    let syn::Variant {
                        attrs: _,
                        ident: var_ident,
                        fields,
                        discriminant: _,
                    } = v;
                    let calls = fields
                        .iter()
                        .map(|f| {
                            let id = f.ident.as_ref().map(|f| quote! {#f:});
                            quote! {#id rows.column_value()?}
                        }).collect::<Vec<_>>();

                    match fields {
                        syn::Fields::Named(_) => quote! { #i => #ident::#var_ident {#(#calls),*}, },
                        syn::Fields::Unnamed(_) => quote! { #i => #ident::#var_ident (#(#calls),*), },
                        syn::Fields::Unit => panic!(),
                    }
                });
            quote! {
                impl #imp Row for #ident #ty #wher {
                    fn try_decode_row<R: ColumnValue>(rows: &mut R) -> anyhow::Result<Self> {
                        Ok(match rows.column_value::<u8>()? {
                            #(#variants)*
                            _ => anyhow::bail!("Invalid variant!"),
                        })
                    }
                }
            }
        }
        syn::Data::Union(_) => panic!("Unions not supported!"),
    };
    res.into()
}

#[proc_macro]
pub fn parse_statement(item: TokenStream) -> TokenStream {
    let statement_str = syn::parse_macro_input!(item as syn::LitStr).value();
    let statement = StatementStream::new(&statement_str).parse::<Statement>().unwrap();
    let res = match &statement {
        Statement::DataDefinition(_) => todo!(),
        Statement::DataManipulation(dml) => {
            let dml = Tokenable(dml);
            quote!(Statement::DataManipulation(#dml).try_into().unwrap())
        },
        Statement::SecondaryIndex(_) => todo!(),
        Statement::MaterializedView(_) => todo!(),
        Statement::Role(_) => todo!(),
        Statement::Permission(_) => todo!(),
        Statement::User(_) => todo!(),
        Statement::UserDefinedFunction(_) => todo!(),
        Statement::UserDefinedType(_) => todo!(),
        Statement::Trigger(_) => todo!(),
    };
    res.into()
}